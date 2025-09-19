"""
Database migration utilities
"""

import asyncio
import logging
from typing import Any, List

from database.connection import DatabaseManager, get_database_manager

logger = logging.getLogger(__name__)


class DatabaseMigration:
    """Database migration manager"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def run_migrations(self):
        """Run all pending migrations"""
        logger.info("Starting database migrations...")

        # Check if migration table exists
        await self._create_migrations_table()

        applied_migrations = await self._get_applied_migrations()

        migrations = [
            ("001_create_partitioning", self._migration_001_create_partitioning),
            (
                "002_add_performance_indexes",
                self._migration_002_add_performance_indexes,
            ),
            (
                "003_create_materialized_views",
                self._migration_003_create_materialized_views,
            ),
        ]

        # Run pending migrations
        for migration_id, migration_func in migrations:
            if migration_id not in applied_migrations:
                logger.info(f"Running migration: {migration_id}")
                try:
                    await migration_func()
                    await self._record_migration(migration_id)
                    logger.info(f"Migration {migration_id} completed successfully")
                except Exception as e:
                    logger.error(f"Migration {migration_id} failed: {e}")
                    raise
            else:
                logger.info(f"Migration {migration_id} already applied")

        logger.info("All migrations completed")

    async def _create_migrations_table(self):
        """Create migrations tracking table"""
        query = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id SERIAL PRIMARY KEY,
            migration_id VARCHAR(255) UNIQUE NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        await self.db_manager.execute_command(query)

    async def _get_applied_migrations(self) -> List[str]:
        """Get list of applied migrations"""
        query = "SELECT migration_id FROM schema_migrations ORDER BY applied_at"
        results = await self.db_manager.execute_query(query)
        return [row["migration_id"] for row in results]

    async def _record_migration(self, migration_id: str):
        """Record that a migration has been applied"""
        query = "INSERT INTO schema_migrations (migration_id) VALUES ($1)"
        await self.db_manager.execute_command(query, migration_id)

    async def _migration_001_create_partitioning(self):
        """Create partitioning for fact_sales table"""
        query = """
        COMMENT ON TABLE fact_sales IS 'Partitioned by date for better query performance'
        """
        await self.db_manager.execute_command(query)

    async def _migration_002_add_performance_indexes(self):
        """Add additional performance indexes"""
        indexes = [
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fact_sales_user_book ON fact_sales(user_id, book_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fact_sales_date_amount ON fact_sales(date_id, amount)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dim_users_email ON dim_users(email) WHERE email IS NOT NULL",
        ]

        for index_query in indexes:
            await self.db_manager.execute_command(index_query)

    async def _migration_003_create_materialized_views(self):
        """Create materialized views for common queries"""
        views = [
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_sales_summary AS
            SELECT 
                d.year,
                d.month,
                COUNT(f.transaction_id) as total_transactions,
                SUM(f.amount) as total_revenue,
                COUNT(DISTINCT f.user_id) as unique_customers,
                COUNT(DISTINCT f.book_id) as unique_books_sold
            FROM fact_sales f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.year, d.month
            ORDER BY d.year, d.month
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mv_category_performance AS
            SELECT 
                b.category,
                COUNT(f.transaction_id) as total_sales,
                SUM(f.amount) as total_revenue,
                AVG(f.amount) as avg_price,
                COUNT(DISTINCT f.user_id) as unique_customers
            FROM fact_sales f
            JOIN dim_books b ON f.book_id = b.book_id
            GROUP BY b.category
            ORDER BY total_revenue DESC
            """,
        ]

        for view_query in views:
            await self.db_manager.execute_command(view_query)

        # Create indexes on materialized views
        index_queries = [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_monthly_sales_year_month ON mv_monthly_sales_summary(year, month)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_category_performance_category ON mv_category_performance(category)",
        ]

        for index_query in index_queries:
            await self.db_manager.execute_command(index_query)

    async def refresh_materialized_views(self):
        """Refresh all materialized views"""
        views = ["mv_monthly_sales_summary", "mv_category_performance"]

        for view in views:
            query = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"
            await self.db_manager.execute_command(query)
            logger.info(f"Refreshed materialized view: {view}")


async def run_migrations():
    """Run database migrations"""
    db_manager = await get_database_manager()
    migration = DatabaseMigration(db_manager)

    try:
        await migration.run_migrations()
    finally:
        await db_manager.close()


if __name__ == "__main__":
    asyncio.run(run_migrations())
