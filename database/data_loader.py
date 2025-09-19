"""
Data loading module for populating the data warehouse
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

import polars as pl

from database.connection import DatabaseManager

logger = logging.getLogger(__name__)


class DataLoader:
    """Data loader for populating the data warehouse"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def load_all_data(
        self,
        users_df: pl.DataFrame,
        transactions_df: pl.DataFrame,
        books_df: pl.DataFrame,
    ):
        """Load all data into the data warehouse"""
        logger.info("Starting data loading process...")

        try:
            # Load dimension tables
            await self._load_dim_date()
            await self._load_dim_users(users_df)
            await self._load_dim_books(books_df)

            # Temporarily disable foreign key constraints for fact table loading
            await self._disable_foreign_key_checks()

            # Load fact tables
            await self._load_fact_sales(transactions_df)

            # Re-enable foreign key constraints
            await self._enable_foreign_key_checks()

            # Create pre-aggregated tables
            await self._create_daily_sales_summary()
            await self._create_book_performance_summary()

            logger.info("Data loading completed successfully!")

        except Exception as e:
            logger.error(f"Data loading failed: {e}")
            # Ensure foreign key constraints are re-enabled even if loading fails
            await self._enable_foreign_key_checks()
            raise

    async def _load_dim_date(self):
        """Load date dimension table"""
        logger.info("Loading date dimension...")

        # Generate date range from 2020 to 2030
        start_date = date(2020, 1, 1)
        end_date = date(2030, 12, 31)

        dates = []
        current_date = start_date

        while current_date <= end_date:
            dates.append(
                {
                    "date_id": int(current_date.strftime("%Y%m%d")),
                    "full_date": current_date,
                    "year": current_date.year,
                    "quarter": (current_date.month - 1) // 3 + 1,
                    "month": current_date.month,
                    "month_name": current_date.strftime("%B"),
                    "day": current_date.day,
                    "day_of_week": current_date.weekday() + 1,
                    "day_name": current_date.strftime("%A"),
                    "is_weekend": current_date.weekday() >= 5,
                    "is_holiday": False,  # Could be enhanced with holiday data
                    "fiscal_year": (
                        current_date.year
                        if current_date.month >= 4
                        else current_date.year - 1
                    ),
                    "fiscal_quarter": (
                        ((current_date.month - 1) // 3 + 1)
                        if current_date.month >= 4
                        else ((current_date.month + 8) // 3 + 1)
                    ),
                }
            )
            current_date += timedelta(days=1)

        # Insert dates in batches
        await self._batch_insert("dim_date", dates)
        logger.info(f"Loaded {len(dates)} date records")

    async def _load_dim_users(self, users_df: pl.DataFrame):
        """Load users dimension table"""
        logger.info("Loading users dimension...")

        users_data = []
        for row in users_df.iter_rows(named=True):
            # Extract state and city from location
            location = row.get("location", "") or ""
            location_parts = location.split(",")
            state = location_parts[-1].strip() if len(location_parts) > 1 else ""
            city = location_parts[0].strip() if location_parts else ""

            # Determine user segment based on signup date (simplified)
            signup_date = row["signup_date"]
            days_since_signup = (date.today() - signup_date).days
            user_segment = (
                "High Value"
                if days_since_signup > 365
                else "Medium Value" if days_since_signup > 90 else "Low Value"
            )

            users_data.append(
                {
                    "user_id": row["id"],
                    "name": row["name"],
                    "email": row["email"],
                    "location": row["location"],
                    "signup_date": signup_date,
                    "social_security_number": row["social_security_number"],
                    "state": state,
                    "city": city,
                    "user_segment": user_segment,
                }
            )

        await self._batch_insert("dim_users", users_data)
        logger.info(f"Loaded {len(users_data)} user records")

    async def _load_dim_books(self, books_df: pl.DataFrame):
        """Load books dimension table"""
        logger.info("Loading books dimension...")

        books_data = []
        for row in books_df.iter_rows(named=True):
            # Determine price tier
            price = row["base_price"]
            if price < 10:
                price_tier = "Low"
            elif price < 25:
                price_tier = "Medium"
            else:
                price_tier = "High"

            # Determine age category
            pub_year = row["publication_year"]
            current_year = datetime.now().year
            if pub_year >= current_year - 5:
                age_category = "Recent"
            elif pub_year >= current_year - 20:
                age_category = "Classic"
            else:
                age_category = "Vintage"

            books_data.append(
                {
                    "book_id": row["book_id"],
                    "title": row["title"],
                    "category": row["category"],
                    "base_price": price,
                    "author": row["author"],
                    "isbn": row["isbn"],
                    "publication_year": pub_year,
                    "pages": row["pages"],
                    "publisher": row["publisher"],
                    "price_tier": price_tier,
                    "age_category": age_category,
                }
            )

        await self._batch_insert("dim_books", books_data)
        logger.info(f"Loaded {len(books_data)} book records")

    async def _load_fact_sales(self, transactions_df: pl.DataFrame):
        """Load sales fact table"""
        logger.info("Loading sales fact table...")

        sales_data = []
        negative_count = 0
        for row in transactions_df.iter_rows(named=True):
            # Skip negative amounts
            if row["amount"] < 0:
                negative_count += 1
                continue

            # Get date_id for the transaction date
            transaction_date = row["timestamp"].date()
            date_id = int(transaction_date.strftime("%Y%m%d"))

            sales_data.append(
                {
                    "transaction_id": row["transaction_id"],
                    "user_id": row["user_id"],
                    "book_id": row["book_id"],
                    "date_id": date_id,
                    "amount": row["amount"],
                    "quantity": 1,  # Assuming 1 book per transaction
                    "discount_amount": 0,  # Could be calculated from base_price vs amount
                    "transaction_timestamp": row["timestamp"],
                }
            )

        if negative_count > 0:
            logger.info(
                f"Filtered out {negative_count} transactions with negative amounts"
            )

        await self._batch_insert("fact_sales", sales_data)
        logger.info(f"Loaded {len(sales_data)} sales records")

    async def _create_daily_sales_summary(self):
        """Create daily sales summary table"""
        logger.info("Creating daily sales summary...")

        query = """
        INSERT INTO fact_daily_sales_summary (
            date_id, total_revenue, transaction_count, unique_users, 
            average_transaction_value, total_quantity
        )
        SELECT 
            f.date_id,
            SUM(f.amount) as total_revenue,
            COUNT(f.transaction_id) as transaction_count,
            COUNT(DISTINCT f.user_id) as unique_users,
            AVG(f.amount) as average_transaction_value,
            SUM(f.quantity) as total_quantity
        FROM fact_sales f
        GROUP BY f.date_id
        ON CONFLICT (date_id) DO UPDATE SET
            total_revenue = EXCLUDED.total_revenue,
            transaction_count = EXCLUDED.transaction_count,
            unique_users = EXCLUDED.unique_users,
            average_transaction_value = EXCLUDED.average_transaction_value,
            total_quantity = EXCLUDED.total_quantity,
            updated_at = CURRENT_TIMESTAMP
        """

        await self.db_manager.execute_command(query)
        logger.info("Daily sales summary created")

    async def _create_book_performance_summary(self):
        """Create book performance summary table"""
        logger.info("Creating book performance summary...")

        query = """
        INSERT INTO fact_book_performance (
            book_id, total_sales, total_revenue, average_price, 
            unique_customers, first_sale_date, last_sale_date
        )
        SELECT 
            f.book_id,
            COUNT(f.transaction_id) as total_sales,
            SUM(f.amount) as total_revenue,
            AVG(f.amount) as average_price,
            COUNT(DISTINCT f.user_id) as unique_customers,
            MIN(d.full_date) as first_sale_date,
            MAX(d.full_date) as last_sale_date
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY f.book_id
        ON CONFLICT (book_id) DO UPDATE SET
            total_sales = EXCLUDED.total_sales,
            total_revenue = EXCLUDED.total_revenue,
            average_price = EXCLUDED.average_price,
            unique_customers = EXCLUDED.unique_customers,
            first_sale_date = EXCLUDED.first_sale_date,
            last_sale_date = EXCLUDED.last_sale_date,
            updated_at = CURRENT_TIMESTAMP
        """

        await self.db_manager.execute_command(query)
        logger.info("Book performance summary created")

    async def _batch_insert(
        self, table_name: str, data: List[Dict[str, Any]], batch_size: int = 1000
    ):
        """Insert data in batches for better performance"""
        if not data:
            return

        columns = list(data[0].keys())
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        columns_str = ", ".join(columns)

        query = f"""
        INSERT INTO {table_name} ({columns_str}) 
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
        """

        # Process in batches
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            batch_values = []

            for row in batch:
                values = [row[col] for col in columns]
                batch_values.append(values)

            # Execute batch insert with error handling
            try:
                async with self.db_manager.get_connection() as conn:
                    await conn.executemany(query, batch_values)
                logger.debug(f"Inserted batch {i//batch_size + 1} for {table_name}")
            except Exception as e:
                logger.warning(
                    f"Failed to insert batch {i//batch_size + 1} for {table_name}: {e}"
                )
                # Try inserting individual rows to identify problematic records
                await self._insert_individual_rows(table_name, batch, columns)

    async def _insert_individual_rows(
        self, table_name: str, batch: List[Dict[str, Any]], columns: List[str]
    ):
        """Insert individual rows to handle foreign key violations gracefully"""
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        columns_str = ", ".join(columns)

        query = f"""
        INSERT INTO {table_name} ({columns_str}) 
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
        """

        successful_inserts = 0
        failed_inserts = 0

        async with self.db_manager.get_connection() as conn:
            for row in batch:
                try:
                    values = [row[col] for col in columns]
                    await conn.execute(query, *values)
                    successful_inserts += 1
                except Exception as e:
                    failed_inserts += 1
                    logger.debug(f"Skipped row due to constraint violation: {e}")

        logger.info(
            f"Individual insert results: {successful_inserts} successful, {failed_inserts} failed"
        )

    async def _disable_foreign_key_checks(self):
        """Temporarily disable foreign key constraint checks"""
        logger.info("Disabling foreign key constraint checks...")
        await self.db_manager.execute_command("SET session_replication_role = replica;")

    async def _enable_foreign_key_checks(self):
        """Re-enable foreign key constraint checks"""
        logger.info("Re-enabling foreign key constraint checks...")
        await self.db_manager.execute_command("SET session_replication_role = DEFAULT;")

    async def refresh_aggregated_tables(self):
        """Refresh pre-aggregated tables with latest data"""
        logger.info("Refreshing aggregated tables...")

        await self._create_daily_sales_summary()
        await self._create_book_performance_summary()

        logger.info("Aggregated tables refreshed")
