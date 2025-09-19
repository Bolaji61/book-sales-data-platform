"""
Optimized analytics queries for Redshift data warehouse
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict

import boto3

logger = logging.getLogger(__name__)


class RedshiftAnalyticsService:
    """Service for Redshift analytics queries"""

    def __init__(
        self,
        cluster_identifier: str,
        database: str,
        db_user: str,
        db_password: str,
        port: int = 5439,
        region: str = "us-east-2",
    ):

        self.cluster_identifier = cluster_identifier
        self.database = database
        self.db_user = db_user
        self.db_password = db_password
        self.port = port
        self.region = region

        # Initialize Redshift Data API client
        self.redshift_client = boto3.client("redshift-data", region_name=self.region)

    async def get_daily_sales_trends(self, days: int = 30) -> Dict[str, Any]:
        """Get daily sales trends for the last N days"""
        query = f"""
        SELECT 
            d.full_date as date,
            d.year,
            d.month,
            d.day_name,
            COUNT(f.transaction_id) as total_transactions,
            SUM(f.amount) as total_revenue,
            AVG(f.amount) as avg_transaction_value,
            COUNT(DISTINCT f.user_id) as unique_customers,
            SUM(f.quantity) as total_books_sold
        FROM dim_date d
        LEFT JOIN fact_sales f ON d.date_id = f.date_id
        WHERE d.full_date >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY d.full_date, d.year, d.month, d.day_name
        ORDER BY d.full_date DESC
        """

        return await self._execute_query(query, "daily_sales_trends")

    async def get_top_books(self, limit: int = 10) -> Dict[str, Any]:
        """Get top performing books by revenue"""
        query = f"""
        SELECT 
            book_id,
            title,
            author,
            category,
            total_sales,
            total_revenue,
            avg_price,
            unique_customers,
            first_sale_date,
            last_sale_date
        FROM mv_top_books
        ORDER BY total_revenue DESC
        LIMIT {limit}
        """

        return await self._execute_query(query, "top_books")

    async def get_user_analytics(self, limit: int = 100) -> Dict[str, Any]:
        """Get user analytics and customer segments"""
        query = f"""
        SELECT 
            user_id,
            name,
            location,
            signup_date,
            total_purchases,
            total_spent,
            avg_purchase_value,
            first_purchase_date,
            last_purchase_date,
            customer_lifespan_days
        FROM mv_user_analytics
        WHERE total_spent > 0
        ORDER BY total_spent DESC
        LIMIT {limit}
        """

        return await self._execute_query(query, "user_analytics")

    async def get_category_performance(self) -> Dict[str, Any]:
        """Get performance by book category"""
        query = """
        SELECT 
            b.category,
            COUNT(f.transaction_id) as total_sales,
            SUM(f.amount) as total_revenue,
            AVG(f.amount) as avg_price,
            COUNT(DISTINCT f.user_id) as unique_customers,
            COUNT(DISTINCT f.book_id) as unique_books
        FROM fact_sales f
        JOIN dim_books b ON f.book_id = b.book_id
        GROUP BY b.category
        ORDER BY total_revenue DESC
        """

        return await self._execute_query(query, "category_performance")

    async def get_sales_summary(self) -> Dict[str, Any]:
        """Get overall sales summary"""
        query = """
        SELECT 
            COUNT(DISTINCT f.transaction_id) as total_transactions,
            SUM(f.amount) as total_revenue,
            AVG(f.amount) as avg_transaction_value,
            COUNT(DISTINCT f.user_id) as total_customers,
            COUNT(DISTINCT f.book_id) as total_books,
            MIN(f.transaction_timestamp) as first_sale_date,
            MAX(f.transaction_timestamp) as last_sale_date,
            COUNT(DISTINCT d.full_date) as days_with_sales
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        """

        return await self._execute_query(query, "sales_summary")

    async def get_monthly_trends(self, months: int = 12) -> Dict[str, Any]:
        """Get monthly sales trends"""
        query = f"""
        SELECT 
            year,
            month,
            month_name,
            COUNT(DISTINCT d.full_date) as days_in_month,
            COUNT(f.transaction_id) as total_transactions,
            SUM(f.amount) as total_revenue,
            AVG(f.amount) as avg_transaction_value,
            COUNT(DISTINCT f.user_id) as unique_customers,
            COUNT(DISTINCT f.book_id) as unique_books
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE d.full_date >= CURRENT_DATE - INTERVAL '{months} months'
        GROUP BY year, month, month_name
        ORDER BY year, month
        """

        return await self._execute_query(query, "monthly_trends")

    async def get_customer_segments(self) -> Dict[str, Any]:
        """Get customer segmentation analysis"""
        query = """
        WITH customer_segments AS (
            SELECT 
                CASE 
                    WHEN total_spent >= 1000 THEN 'High Value'
                    WHEN total_spent >= 500 THEN 'Medium Value'
                    WHEN total_spent >= 100 THEN 'Low Value'
                    ELSE 'New Customer'
                END as segment,
                COUNT(*) as customer_count,
                SUM(total_spent) as total_revenue,
                AVG(total_spent) as avg_spent,
                AVG(total_purchases) as avg_purchases
            FROM mv_user_analytics
            GROUP BY 
                CASE 
                    WHEN total_spent >= 1000 THEN 'High Value'
                    WHEN total_spent >= 500 THEN 'Medium Value'
                    WHEN total_spent >= 100 THEN 'Low Value'
                    ELSE 'New Customer'
                END
        )
        SELECT 
            segment,
            customer_count,
            total_revenue,
            avg_spent,
            avg_purchases,
            ROUND((customer_count * 100.0 / SUM(customer_count) OVER()), 2) as percentage
        FROM customer_segments
        ORDER BY total_revenue DESC
        """

        return await self._execute_query(query, "customer_segments")

    async def get_author_performance(self, limit: int = 20) -> Dict[str, Any]:
        """Get top performing authors"""
        query = f"""
        SELECT 
            b.author,
            COUNT(DISTINCT b.book_id) as books_written,
            COUNT(f.transaction_id) as total_sales,
            SUM(f.amount) as total_revenue,
            AVG(f.amount) as avg_price,
            COUNT(DISTINCT f.user_id) as unique_customers
        FROM fact_sales f
        JOIN dim_books b ON f.book_id = b.book_id
        GROUP BY b.author
        ORDER BY total_revenue DESC
        LIMIT {limit}
        """

        return await self._execute_query(query, "author_performance")

    async def _execute_query(self, query: str, query_type: str) -> Dict[str, Any]:
        """Execute query and return results"""
        try:
            logger.info(f"Executing {query_type} query...")

            response = self.redshift_client.execute_statement(
                ClusterIdentifier=self.cluster_identifier,
                Database=self.database,
                DbUser=self.db_user,
                Sql=query,
            )

            # Wait for completion
            query_id = response["Id"]
            while True:
                result = self.redshift_client.describe_statement(Id=query_id)
                status = result["Status"]
                if status in ["FINISHED", "FAILED", "ABORTED"]:
                    break
                await asyncio.sleep(1)

            if status != "FINISHED":
                error_msg = result.get("Error", "Unknown error")
                raise Exception(f"Query failed: {error_msg}")

            # Get query results
            results = self.redshift_client.get_statement_result(Id=query_id)

            columns = [col["name"] for col in results["ColumnMetadata"]]
            rows = []

            for record in results["Records"]:
                row = {}
                for i, value in enumerate(record):
                    if "stringValue" in value:
                        row[columns[i]] = value["stringValue"]
                    elif "longValue" in value:
                        row[columns[i]] = value["longValue"]
                    elif "doubleValue" in value:
                        row[columns[i]] = value["doubleValue"]
                    elif "booleanValue" in value:
                        row[columns[i]] = value["booleanValue"]
                    else:
                        row[columns[i]] = None
                rows.append(row)

            logger.info(f"Query {query_type} completed successfully")

            return {
                "data": rows,
                "query_type": query_type,
                "timestamp": datetime.now().isoformat(),
                "row_count": len(rows),
            }

        except Exception as e:
            logger.error(f"Failed to execute {query_type} query: {e}")
            raise
