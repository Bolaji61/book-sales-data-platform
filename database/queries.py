"""
Database query definitions for the book sales data warehouse
"""

from datetime import date
from typing import Any, Dict, List, Optional

from database.connection import DatabaseManager


class DatabaseQueries:
    """Database query definitions"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    # user queries
    async def get_users(
        self, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get users with pagination"""
        query = """
        SELECT user_id, name, email, location, signup_date, 
               social_security_number, state, city, user_segment
        FROM dim_users
        ORDER BY user_id
        LIMIT $1 OFFSET $2
        """
        return await self.db_manager.execute_query(query, limit, offset)

    async def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        query = """
        SELECT user_id, name, email, location, signup_date, 
               social_security_number, state, city, user_segment
        FROM dim_users
        WHERE user_id = $1
        """
        results = await self.db_manager.execute_query(query, user_id)
        return results[0] if results else None

    async def get_user_transactions(
        self, user_id: int, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get user's transaction history"""
        query = """
        SELECT f.transaction_id, f.amount, f.transaction_timestamp,
               b.title as book_title, b.category as book_category,
               b.author as book_author, b.base_price
        FROM fact_sales f
        JOIN dim_books b ON f.book_id = b.book_id
        WHERE f.user_id = $1
        ORDER BY f.transaction_timestamp DESC
        LIMIT $2
        """
        return await self.db_manager.execute_query(query, user_id, limit)

    # book queries
    async def get_books(
        self, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get books with pagination"""
        query = """
        SELECT book_id, title, category, base_price, author, 
               isbn, publication_year, pages, publisher, 
               price_tier, age_category
        FROM dim_books
        ORDER BY book_id
        LIMIT $1 OFFSET $2
        """
        return await self.db_manager.execute_query(query, limit, offset)

    async def get_book_by_id(self, book_id: int) -> Optional[Dict[str, Any]]:
        """Get book by ID"""
        query = """
        SELECT book_id, title, category, base_price, author, 
               isbn, publication_year, pages, publisher, 
               price_tier, age_category
        FROM dim_books
        WHERE book_id = $1
        """
        results = await self.db_manager.execute_query(query, book_id)
        return results[0] if results else None

    async def get_books_by_category(
        self, category: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get books by category"""
        query = """
        SELECT b.book_id, b.title, b.category, b.base_price, b.author,
               bp.total_sales, bp.total_revenue, bp.average_price
        FROM dim_books b
        LEFT JOIN fact_book_performance bp ON b.book_id = bp.book_id
        WHERE b.category = $1
        ORDER BY bp.total_revenue DESC NULLS LAST
        LIMIT $2
        """
        return await self.db_manager.execute_query(query, category, limit)

    # sales queries
    async def get_daily_sales_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get daily sales summary"""
        query = """
        SELECT d.full_date, ds.total_revenue, ds.transaction_count, 
               ds.unique_users, ds.average_transaction_value, ds.total_quantity
        FROM fact_daily_sales_summary ds
        JOIN dim_date d ON ds.date_id = d.date_id
        WHERE 1=1
        """
        params = []
        param_count = 0

        if start_date:
            param_count += 1
            query += f" AND d.full_date >= ${param_count}"
            params.append(start_date)

        if end_date:
            param_count += 1
            query += f" AND d.full_date <= ${param_count}"
            params.append(end_date)

        query += " ORDER BY d.full_date DESC LIMIT $" + str(param_count + 1)
        params.append(limit)

        return await self.db_manager.execute_query(query, *params)

    async def get_top_books(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get top performing books"""
        query = """
        SELECT b.book_id, b.title, b.category, b.author,
               bp.total_sales, bp.total_revenue, bp.average_price,
               bp.unique_customers, bp.first_sale_date, bp.last_sale_date
        FROM fact_book_performance bp
        JOIN dim_books b ON bp.book_id = b.book_id
        ORDER BY bp.total_revenue DESC
        LIMIT $1
        """
        return await self.db_manager.execute_query(query, limit)

    async def get_sales_by_category(
        self, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """Get sales performance by category"""
        query = """
        SELECT b.category, 
               COUNT(f.transaction_id) as total_transactions,
               SUM(f.amount) as total_revenue,
               AVG(f.amount) as average_transaction_value,
               COUNT(DISTINCT f.user_id) as unique_customers
        FROM fact_sales f
        JOIN dim_books b ON f.book_id = b.book_id
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE 1=1
        """
        params = []
        param_count = 0

        if start_date:
            param_count += 1
            query += f" AND d.full_date >= ${param_count}"
            params.append(start_date)

        if end_date:
            param_count += 1
            query += f" AND d.full_date <= ${param_count}"
            params.append(end_date)

        query += """
        GROUP BY b.category
        ORDER BY total_revenue DESC
        """

        return await self.db_manager.execute_query(query, *params)

    # analytics queries
    async def get_analytics_overview(self) -> Dict[str, Any]:
        """Get overall analytics overview"""
        query = """
        SELECT 
            (SELECT COUNT(*) FROM dim_users) as total_users,
            (SELECT COUNT(*) FROM fact_sales) as total_transactions,
            (SELECT SUM(amount) FROM fact_sales) as total_revenue,
            (SELECT COUNT(*) FROM dim_books) as total_books,
            (SELECT AVG(amount) FROM fact_sales) as average_transaction_value,
            (SELECT category FROM (
                SELECT b.category, SUM(f.amount) as category_revenue
                FROM fact_sales f
                JOIN dim_books b ON f.book_id = b.book_id
                GROUP BY b.category
                ORDER BY category_revenue DESC
                LIMIT 1
            ) top_category) as top_category,
            (SELECT user_id FROM (
                SELECT user_id, COUNT(*) as transaction_count
                FROM fact_sales
                GROUP BY user_id
                ORDER BY transaction_count DESC
                LIMIT 1
            ) top_user) as most_active_user
        """
        results = await self.db_manager.execute_query(query)
        return results[0] if results else {}

    async def get_user_behavior_analytics(self) -> Dict[str, Any]:
        """Get user behavior analytics"""
        # user segments
        segments_query = """
        SELECT user_segment, COUNT(*) as count
        FROM dim_users
        GROUP BY user_segment
        """
        segments = await self.db_manager.execute_query(segments_query)

        # purchase patterns by hour
        hourly_query = """
        SELECT EXTRACT(HOUR FROM transaction_timestamp) as hour,
               COUNT(*) as transaction_count
        FROM fact_sales
        GROUP BY EXTRACT(HOUR FROM transaction_timestamp)
        ORDER BY hour
        """
        hourly_patterns = await self.db_manager.execute_query(hourly_query)

        # geographic distribution
        geo_query = """
        SELECT state, COUNT(*) as user_count
        FROM dim_users
        WHERE state IS NOT NULL AND state != ''
        GROUP BY state
        ORDER BY user_count DESC
        LIMIT 10
        """
        geographic = await self.db_manager.execute_query(geo_query)

        return {
            "user_segments": {seg["user_segment"]: seg["count"] for seg in segments},
            "purchase_patterns": {"hourly_distribution": hourly_patterns},
            "retention_metrics": {
                "monthly_retention": 0.75,  # Placeholder - would need historical data
                "quarterly_retention": 0.60,
                "annual_retention": 0.45,
            },
            "geographic_distribution": {
                geo["state"]: geo["user_count"] for geo in geographic
            },
        }

    async def get_sales_trends(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get sales trends for the last N days"""
        query = (
            """
        SELECT d.full_date, ds.total_revenue, ds.transaction_count,
               ds.unique_users, ds.average_transaction_value
        FROM fact_daily_sales_summary ds
        JOIN dim_date d ON ds.date_id = d.date_id
        WHERE d.full_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY d.full_date DESC
        """
            % days
        )

        return await self.db_manager.execute_query(query)

    async def get_top_customers(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get top customers by spending"""
        query = """
        SELECT u.user_id, u.name, u.user_segment,
               COUNT(f.transaction_id) as total_transactions,
               SUM(f.amount) as total_spent,
               AVG(f.amount) as avg_transaction_value,
               COUNT(DISTINCT f.book_id) as unique_books_purchased
        FROM dim_users u
        JOIN fact_sales f ON u.user_id = f.user_id
        GROUP BY u.user_id, u.name, u.user_segment
        ORDER BY total_spent DESC
        LIMIT $1
        """
        return await self.db_manager.execute_query(query, limit)

    # performance queries
    async def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        queries = {
            "table_sizes": """
                SELECT schemaname, tablename, 
                       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            """,
            "index_usage": """
                SELECT schemaname, tablename, indexname, idx_tup_read, idx_tup_fetch
                FROM pg_stat_user_indexes
                ORDER BY idx_tup_read DESC
            """,
            "table_stats": """
                SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del, n_live_tup
                FROM pg_stat_user_tables
                ORDER BY n_live_tup DESC
            """,
        }

        results = {}
        for key, query in queries.items():
            results[key] = await self.db_manager.execute_query(query)

        return results
