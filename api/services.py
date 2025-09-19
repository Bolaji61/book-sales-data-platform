"""
API service classes for the Book Sales Data Service
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from api.models import (CategoryPerformance, CustomerSegment, SalesData,
                        SalesQueryParams, TopBookData, TopBooksQueryParams,
                        TrendAnalysis, UserAnalytics, UserHistoryQueryParams,
                        UserPurchaseData)
from database.queries import DatabaseQueries

logger = logging.getLogger(__name__)


class SalesService:
    """Service for sales-related API operations"""

    def __init__(self, db_queries: DatabaseQueries):
        self.db_queries = db_queries

    async def get_daily_sales(
        self, params: SalesQueryParams
    ) -> Tuple[List[SalesData], Dict[str, Any]]:
        """Get daily sales data with filtering and pagination"""
        try:
            # Build query with filters
            query = """
            SELECT 
                d.full_date as date,
                ds.total_revenue,
                ds.transaction_count,
                ds.unique_users as unique_customers,
                ds.average_transaction_value,
                ds.total_quantity as total_books_sold
            FROM fact_daily_sales_summary ds
            JOIN dim_date d ON ds.date_id = d.date_id
            WHERE 1=1
            """

            query_params = []
            param_count = 0

            # Add date filters
            if params.date_range.start_date:
                param_count += 1
                query += f" AND d.full_date >= ${param_count}"
                query_params.append(params.date_range.start_date)

            if params.date_range.end_date:
                param_count += 1
                query += f" AND d.full_date <= ${param_count}"
                query_params.append(params.date_range.end_date)

            # Add category filter if specified
            if params.category:
                param_count += 1
                query += f"""
                AND EXISTS (
                    SELECT 1 FROM fact_sales f
                    JOIN dim_books b ON f.book_id = b.book_id
                    WHERE f.date_id = ds.date_id AND b.category = ${param_count}
                )
                """
                query_params.append(params.category)

            # Add user segment filter if specified
            if params.user_segment:
                param_count += 1
                query += f"""
                AND EXISTS (
                    SELECT 1 FROM fact_sales f
                    JOIN dim_users u ON f.user_id = u.user_id
                    WHERE f.date_id = ds.date_id AND u.user_segment = ${param_count}
                )
                """
                query_params.append(params.user_segment)

            # Get total count for pagination
            count_query = f"SELECT COUNT(*) as total FROM ({query}) as filtered_data"
            total_count = await self.db_queries.db_manager.execute_query(
                count_query, *query_params
            )
            total_records = total_count[0]["total"] if total_count else 0

            # Add ordering and pagination
            query += f" ORDER BY d.full_date DESC LIMIT ${param_count + 1} OFFSET ${param_count + 2}"
            query_params.extend([params.pagination.limit, params.pagination.offset])

            # Execute query
            results = await self.db_queries.db_manager.execute_query(
                query, *query_params
            )

            # Convert to response models
            sales_data = [
                SalesData(
                    date=row["date"],
                    total_revenue=row["total_revenue"],
                    transaction_count=row["transaction_count"],
                    unique_customers=row["unique_customers"],
                    average_transaction_value=row["average_transaction_value"],
                    total_books_sold=row["total_books_sold"],
                )
                for row in results
            ]

            # Calculate summary statistics
            summary = await self._calculate_sales_summary(params)

            return sales_data, {
                "total_records": total_records,
                "pagination": {
                    "limit": params.pagination.limit,
                    "offset": params.pagination.offset,
                    "has_more": params.pagination.offset + params.pagination.limit
                    < total_records,
                },
                "summary": summary,
            }

        except Exception as e:
            logger.error(f"Error getting daily sales: {e}")
            raise

    async def _calculate_sales_summary(
        self, params: SalesQueryParams
    ) -> Dict[str, Any]:
        """Calculate summary statistics for sales data"""
        try:
            # Build summary query
            query = """
            SELECT 
                SUM(ds.total_revenue) as total_revenue,
                SUM(ds.transaction_count) as total_transactions,
                AVG(ds.average_transaction_value) as avg_transaction_value,
                COUNT(DISTINCT ds.date_id) as days_with_sales
            FROM fact_daily_sales_summary ds
            JOIN dim_date d ON ds.date_id = d.date_id
            WHERE 1=1
            """

            query_params = []
            param_count = 0

            # Add same filters as main query
            if params.date_range.start_date:
                param_count += 1
                query += f" AND d.full_date >= ${param_count}"
                query_params.append(params.date_range.start_date)

            if params.date_range.end_date:
                param_count += 1
                query += f" AND d.full_date <= ${param_count}"
                query_params.append(params.date_range.end_date)

            if params.category:
                param_count += 1
                query += f"""
                AND EXISTS (
                    SELECT 1 FROM fact_sales f
                    JOIN dim_books b ON f.book_id = b.book_id
                    WHERE f.date_id = ds.date_id AND b.category = ${param_count}
                )
                """
                query_params.append(params.category)

            if params.user_segment:
                param_count += 1
                query += f"""
                AND EXISTS (
                    SELECT 1 FROM fact_sales f
                    JOIN dim_users u ON f.user_id = u.user_id
                    WHERE f.date_id = ds.date_id AND u.user_segment = ${param_count}
                )
                """
                query_params.append(params.user_segment)

            results = await self.db_queries.db_manager.execute_query(
                query, *query_params
            )
            return results[0] if results else {}

        except Exception as e:
            logger.error(f"Error calculating sales summary: {e}")
            return {}


class TopBooksService:
    """Service for top books API operations"""

    def __init__(self, db_queries: DatabaseQueries):
        self.db_queries = db_queries

    async def get_top_books(self, params: TopBooksQueryParams) -> List[TopBookData]:
        """Get top books by specified metric"""
        try:
            # Build query based on metric
            if params.metric.value == "revenue":
                order_by = "bp.total_revenue DESC"
                metric_value = "bp.total_revenue"
            elif params.metric.value == "sales_count":
                order_by = "bp.total_sales DESC"
                metric_value = "bp.total_sales"
            elif params.metric.value == "customers":
                order_by = "bp.unique_customers DESC"
                metric_value = "bp.unique_customers"
            else:  # books_sold
                order_by = "bp.total_sales DESC"
                metric_value = "bp.total_sales"

            query = f"""
            SELECT 
                b.book_id,
                b.title,
                b.author,
                b.category,
                bp.total_revenue,
                bp.total_sales,
                bp.average_price,
                bp.unique_customers,
                ROW_NUMBER() OVER (ORDER BY {order_by}) as rank
            FROM fact_book_performance bp
            JOIN dim_books b ON bp.book_id = b.book_id
            WHERE 1=1
            """

            query_params = []
            param_count = 0

            # Add category filter
            if params.category:
                param_count += 1
                query += f" AND b.category = ${param_count}"
                query_params.append(params.category)

            # Add time range filter if specified
            if params.time_range:
                param_count += 1
                if params.time_range.value == "daily":
                    date_filter = "d.full_date = CURRENT_DATE"
                elif params.time_range.value == "weekly":
                    date_filter = "d.full_date >= CURRENT_DATE - INTERVAL '7 days'"
                elif params.time_range.value == "monthly":
                    date_filter = "d.full_date >= CURRENT_DATE - INTERVAL '30 days'"
                else:  # yearly
                    date_filter = "d.full_date >= CURRENT_DATE - INTERVAL '1 year'"

                query = f"""
                SELECT 
                    b.book_id,
                    b.title,
                    b.author,
                    b.category,
                    SUM(f.amount) as total_revenue,
                    COUNT(f.transaction_id) as total_sales,
                    AVG(f.amount) as average_price,
                    COUNT(DISTINCT f.user_id) as unique_customers,
                    ROW_NUMBER() OVER (ORDER BY SUM(f.amount) DESC) as rank
                FROM fact_sales f
                JOIN dim_books b ON f.book_id = b.book_id
                JOIN dim_date d ON f.date_id = d.date_id
                WHERE {date_filter}
                """

                if params.category:
                    query += f" AND b.category = ${param_count}"
                    query_params.append(params.category)

                query += f" GROUP BY b.book_id, b.title, b.author, b.category"

            query += f" ORDER BY {order_by} LIMIT ${param_count + 1}"
            query_params.append(params.limit)

            results = await self.db_queries.db_manager.execute_query(
                query, *query_params
            )

            return [
                TopBookData(
                    book_id=row["book_id"],
                    title=row["title"],
                    author=row["author"],
                    category=row["category"],
                    total_revenue=row["total_revenue"],
                    total_sales=row["total_sales"],
                    average_price=row["average_price"],
                    unique_customers=row["unique_customers"],
                    rank=row["rank"],
                )
                for row in results
            ]

        except Exception as e:
            logger.error(f"Error getting top books: {e}")
            raise


class UserHistoryService:
    """Service for user history API operations"""

    def __init__(self, db_queries: DatabaseQueries):
        self.db_queries = db_queries

    async def get_user_history(
        self, user_id: int, params: UserHistoryQueryParams
    ) -> Tuple[List[UserPurchaseData], Optional[UserAnalytics], Dict[str, Any]]:
        """Get user purchase history with analytics"""
        try:
            # Get user information
            user_info = await self.db_queries.get_user_by_id(user_id)
            if not user_info:
                raise ValueError(f"User {user_id} not found")

            # Build purchase history query
            query = """
            SELECT 
                f.transaction_id,
                f.transaction_timestamp as transaction_date,
                b.title as book_title,
                b.category as book_category,
                b.author as book_author,
                f.amount,
                f.quantity
            FROM fact_sales f
            JOIN dim_books b ON f.book_id = b.book_id
            WHERE f.user_id = $1
            """

            query_params = [user_id]
            param_count = 1

            # Add date filters
            if params.date_range.start_date:
                param_count += 1
                query += f" AND f.transaction_timestamp >= ${param_count}"
                query_params.append(params.date_range.start_date)

            if params.date_range.end_date:
                param_count += 1
                query += f" AND f.transaction_timestamp <= ${param_count}"
                query_params.append(params.date_range.end_date)

            # Get total count
            count_query = f"SELECT COUNT(*) as total FROM ({query}) as filtered_data"
            total_count = await self.db_queries.db_manager.execute_query(
                count_query, *query_params
            )
            total_records = total_count[0]["total"] if total_count else 0

            # Add ordering and pagination
            query += f" ORDER BY f.transaction_timestamp DESC LIMIT ${param_count + 1} OFFSET ${param_count + 2}"
            query_params.extend([params.pagination.limit, params.pagination.offset])

            # Execute query
            results = await self.db_queries.db_manager.execute_query(
                query, *query_params
            )

            # Convert to response models
            purchases = [
                UserPurchaseData(
                    transaction_id=row["transaction_id"],
                    transaction_date=row["transaction_date"],
                    book_title=row["book_title"],
                    book_category=row["book_category"],
                    book_author=row["book_author"],
                    amount=row["amount"],
                    quantity=row["quantity"],
                )
                for row in results
            ]

            # Get analytics if requested
            analytics = None
            if params.include_analytics:
                analytics = await self._get_user_analytics(user_id, params.date_range)

            pagination_info = {
                "total_records": total_records,
                "limit": params.pagination.limit,
                "offset": params.pagination.offset,
                "has_more": params.pagination.offset + params.pagination.limit
                < total_records,
            }

            return purchases, analytics, pagination_info

        except Exception as e:
            logger.error(f"Error getting user history: {e}")
            raise

    async def _get_user_analytics(self, user_id: int, date_range) -> UserAnalytics:
        """Get user analytics summary"""
        try:
            query = """
            SELECT 
                COUNT(f.transaction_id) as total_transactions,
                SUM(f.amount) as total_spent,
                AVG(f.amount) as average_transaction_value,
                MIN(f.transaction_timestamp) as first_purchase_date,
                MAX(f.transaction_timestamp) as last_purchase_date,
                COUNT(DISTINCT f.book_id) as unique_books_purchased,
                COUNT(DISTINCT b.category) as unique_categories
            FROM fact_sales f
            JOIN dim_books b ON f.book_id = b.book_id
            WHERE f.user_id = $1
            """

            query_params = [user_id]
            param_count = 1

            # Add date filters
            if date_range.start_date:
                param_count += 1
                query += f" AND f.transaction_timestamp >= ${param_count}"
                query_params.append(date_range.start_date)

            if date_range.end_date:
                param_count += 1
                query += f" AND f.transaction_timestamp <= ${param_count}"
                query_params.append(date_range.end_date)

            # Get user segment
            user_segment_query = "SELECT user_segment FROM dim_users WHERE user_id = $1"
            user_segment_result = await self.db_queries.db_manager.execute_query(
                user_segment_query, user_id
            )
            user_segment = (
                user_segment_result[0]["user_segment"]
                if user_segment_result
                else "Unknown"
            )

            # Get favorite category
            category_query = f"""
            SELECT b.category, COUNT(*) as purchase_count
            FROM fact_sales f
            JOIN dim_books b ON f.book_id = b.book_id
            WHERE f.user_id = $1
            GROUP BY b.category
            ORDER BY purchase_count DESC
            LIMIT 1
            """
            category_result = await self.db_queries.db_manager.execute_query(
                category_query, user_id
            )
            favorite_category = (
                category_result[0]["category"] if category_result else None
            )

            # Execute main analytics query
            results = await self.db_queries.db_manager.execute_query(
                query, *query_params
            )
            result = results[0] if results else {}

            return UserAnalytics(
                total_transactions=result.get("total_transactions", 0),
                total_spent=result.get("total_spent", 0.0),
                average_transaction_value=result.get("average_transaction_value", 0.0),
                first_purchase_date=result.get("first_purchase_date"),
                last_purchase_date=result.get("last_purchase_date"),
                unique_books_purchased=result.get("unique_books_purchased", 0),
                favorite_category=favorite_category,
                user_segment=user_segment,
            )

        except Exception as e:
            logger.error(f"Error getting user analytics: {e}")
            return UserAnalytics(
                total_transactions=0,
                total_spent=0.0,
                average_transaction_value=0.0,
                first_purchase_date=None,
                last_purchase_date=None,
                unique_books_purchased=0,
                favorite_category=None,
                user_segment="Unknown",
            )


class AnalyticsService:
    """Service for advanced analytics operations"""

    def __init__(self, db_queries: DatabaseQueries):
        self.db_queries = db_queries

    async def get_category_performance(
        self, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> List[CategoryPerformance]:
        """Get category performance analysis"""
        try:
            query = """
            SELECT 
                b.category,
                SUM(f.amount) as total_revenue,
                COUNT(f.transaction_id) as total_sales,
                COUNT(DISTINCT f.user_id) as unique_customers,
                AVG(f.amount) as average_price
            FROM fact_sales f
            JOIN dim_books b ON f.book_id = b.book_id
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE 1=1
            """

            query_params = []
            param_count = 0

            if start_date:
                param_count += 1
                query += f" AND d.full_date >= ${param_count}"
                query_params.append(start_date)

            if end_date:
                param_count += 1
                query += f" AND d.full_date <= ${param_count}"
                query_params.append(end_date)

            query += " GROUP BY b.category ORDER BY total_revenue DESC"

            results = await self.db_queries.db_manager.execute_query(
                query, *query_params
            )

            # Calculate total revenue for market share
            total_revenue = sum(row["total_revenue"] for row in results)

            return [
                CategoryPerformance(
                    category=row["category"],
                    total_revenue=row["total_revenue"],
                    total_sales=row["total_sales"],
                    unique_customers=row["unique_customers"],
                    average_price=row["average_price"],
                    market_share=(
                        (row["total_revenue"] / total_revenue * 100)
                        if total_revenue > 0
                        else 0
                    ),
                )
                for row in results
            ]

        except Exception as e:
            logger.error(f"Error getting category performance: {e}")
            return []

    async def get_customer_segments(self) -> List[CustomerSegment]:
        """Get customer segment analysis"""
        try:
            query = """
            SELECT 
                u.user_segment,
                COUNT(DISTINCT u.user_id) as customer_count,
                SUM(f.amount) as total_revenue,
                AVG(f.amount) as average_order_value,
                COUNT(f.transaction_id) as total_orders
            FROM dim_users u
            LEFT JOIN fact_sales f ON u.user_id = f.user_id
            GROUP BY u.user_segment
            ORDER BY total_revenue DESC
            """

            results = await self.db_queries.db_manager.execute_query(query)

            return [
                CustomerSegment(
                    segment=row["user_segment"],
                    customer_count=row["customer_count"],
                    total_revenue=row["total_revenue"] or 0,
                    average_order_value=row["average_order_value"] or 0,
                    retention_rate=0.75,  # Placeholder - would need historical data
                    lifetime_value=(
                        (row["total_revenue"] or 0) / row["customer_count"]
                        if row["customer_count"] > 0
                        else 0
                    ),
                )
                for row in results
            ]

        except Exception as e:
            logger.error(f"Error getting customer segments: {e}")
            return []
