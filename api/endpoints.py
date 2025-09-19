"""
API endpoints for Book Sales Data Service
"""

import logging
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from api.models import (CategoryPerformance, ComprehensiveAnalytics,
                        CustomerSegment, DateRangeParams, ErrorResponse,
                        PaginationParams, SalesQueryParams, SalesResponse,
                        TopBooksQueryParams, TopBooksResponse,
                        UserHistoryQueryParams, UserHistoryResponse)
from api.services import (AnalyticsService, SalesService, TopBooksService,
                          UserHistoryService)
from database.queries import DatabaseQueries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Book Sales API"])


async def get_db_queries() -> DatabaseQueries:
    """Dependency to get database queries instance"""
    raise HTTPException(status_code=503, detail="Database not initialized")


@router.get("/sales/daily", response_model=SalesResponse)
async def get_daily_sales(
    limit: int = Query(
        default=100, ge=1, le=1000, description="Number of records to return"
    ),
    offset: int = Query(default=0, ge=0, description="Number of records to skip"),
    start_date: Optional[date] = Query(
        default=None, description="Start date for filtering"
    ),
    end_date: Optional[date] = Query(
        default=None, description="End date for filtering"
    ),
    category: Optional[str] = Query(
        default=None, description="Filter by book category"
    ),
    user_segment: Optional[str] = Query(
        default=None, description="Filter by user segment"
    ),
    db_queries: DatabaseQueries = Depends(get_db_queries),
):
    """
    Get daily sales data with comprehensive filtering and pagination.

    This endpoint provides detailed daily sales information including:
    - Total revenue per day
    - Transaction counts
    - Unique customer counts
    - Average transaction values
    - Total books sold

    Supports filtering by date range, category, and user segment.
    """
    try:
        # Build query parameters
        params = SalesQueryParams(
            pagination=PaginationParams(limit=limit, offset=offset),
            date_range=DateRangeParams(start_date=start_date, end_date=end_date),
            category=category,
            user_segment=user_segment,
        )

        # Get sales data
        sales_service = SalesService(db_queries)
        sales_data, metadata = await sales_service.get_daily_sales(params)

        return SalesResponse(
            data=sales_data,
            total_records=metadata["total_records"],
            pagination=metadata["pagination"],
            summary=metadata["summary"],
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error in get_daily_sales: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sales/summary")
async def get_sales_summary(
    start_date: Optional[date] = Query(
        default=None, description="Start date for summary"
    ),
    end_date: Optional[date] = Query(default=None, description="End date for summary"),
    category: Optional[str] = Query(default=None, description="Filter by category"),
    db_queries: DatabaseQueries = Depends(get_db_queries),
):
    """
    Get comprehensive sales summary for the specified period.

    Returns aggregated metrics including:
    - Total revenue and transaction counts
    - Average transaction values
    - Customer and book statistics
    - Category breakdowns
    """
    try:
        params = SalesQueryParams(
            pagination=PaginationParams(limit=1000, offset=0),
            date_range=DateRangeParams(start_date=start_date, end_date=end_date),
            category=category,
        )

        sales_service = SalesService(db_queries)
        _, metadata = await sales_service.get_daily_sales(params)

        return {
            "summary": metadata["summary"],
            "filters_applied": {
                "start_date": start_date,
                "end_date": end_date,
                "category": category,
            },
        }

    except Exception as e:
        logger.error(f"Error in get_sales_summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/books/top", response_model=TopBooksResponse)
async def get_top_books(
    limit: int = Query(
        default=5, ge=1, le=100, description="Number of top books to return"
    ),
    metric: str = Query(
        default="revenue",
        description="Metric to rank by (revenue, sales_count, customers)",
    ),
    category: Optional[str] = Query(
        default=None, description="Filter by book category"
    ),
    time_range: Optional[str] = Query(
        default=None, description="Time range (daily, weekly, monthly, yearly)"
    ),
    db_queries: DatabaseQueries = Depends(get_db_queries),
):
    """
    Get top performing books by specified metric.

    Supports ranking by:
    - Revenue (total sales amount)
    - Sales count (number of transactions)
    - Customers (unique customer count)

    Optional filtering by category and time range.
    """
    try:
        # Validate metric
        valid_metrics = ["revenue", "sales_count", "customers", "books_sold"]
        if metric not in valid_metrics:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid metric. Must be one of: {valid_metrics}",
            )

        # Validate time range
        valid_time_ranges = ["daily", "weekly", "monthly", "yearly"]
        if time_range and time_range not in valid_time_ranges:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid time_range. Must be one of: {valid_time_ranges}",
            )

        # Build query parameters
        params = TopBooksQueryParams(
            limit=limit, metric=metric, category=category, time_range=time_range
        )

        # Get top books
        top_books_service = TopBooksService(db_queries)
        top_books = await top_books_service.get_top_books(params)

        return TopBooksResponse(
            data=top_books,
            metric_used=metric,
            time_range=time_range,
            category_filter=category,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error in get_top_books: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/books/top-by-category")
async def get_top_books_by_category(
    category: str = Query(description="Book category to analyze"),
    limit: int = Query(
        default=10, ge=1, le=50, description="Number of books to return"
    ),
    db_queries: DatabaseQueries = Depends(get_db_queries),
):
    """
    Get top books within a specific category.

    Returns the best performing books in the specified category,
    ranked by total revenue.
    """
    try:
        params = TopBooksQueryParams(limit=limit, metric="revenue", category=category)

        top_books_service = TopBooksService(db_queries)
        top_books = await top_books_service.get_top_books(params)

        return {
            "category": category,
            "books": top_books,
            "total_books_found": len(top_books),
        }

    except Exception as e:
        logger.error(f"Error in get_top_books_by_category: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/users/{user_id}/history", response_model=UserHistoryResponse)
async def get_user_purchase_history(
    user_id: int = Path(description="User ID to get history for"),
    limit: int = Query(
        default=100, ge=1, le=1000, description="Number of purchases to return"
    ),
    offset: int = Query(default=0, ge=0, description="Number of purchases to skip"),
    start_date: Optional[date] = Query(
        default=None, description="Start date for filtering"
    ),
    end_date: Optional[date] = Query(
        default=None, description="End date for filtering"
    ),
    include_analytics: bool = Query(default=True, description="Include user analytics"),
    db_queries: DatabaseQueries = Depends(get_db_queries),
):
    """
    Get comprehensive user purchase history with analytics.

    Returns:
    - Individual purchase records with book details
    - User analytics summary (if requested)
    - Pagination information

    Supports filtering by date range and includes detailed analytics
    about the user's purchasing behavior.
    """
    try:
        # Build query parameters
        params = UserHistoryQueryParams(
            pagination=PaginationParams(limit=limit, offset=offset),
            date_range=DateRangeParams(start_date=start_date, end_date=end_date),
            include_analytics=include_analytics,
        )

        # Get user history
        user_history_service = UserHistoryService(db_queries)
        purchases, analytics, pagination_info = (
            await user_history_service.get_user_history(user_id, params)
        )

        # Get user information
        user_info = await db_queries.get_user_by_id(user_id)
        if not user_info:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")

        return UserHistoryResponse(
            user_id=user_id,
            user_name=user_info["name"],
            user_email=user_info["email"],
            purchases=purchases,
            analytics=analytics,
            pagination=pagination_info,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error in get_user_purchase_history: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/users/{user_id}/analytics")
async def get_user_analytics(
    user_id: int = Path(description="User ID to get analytics for"),
    start_date: Optional[date] = Query(
        default=None, description="Start date for analytics"
    ),
    end_date: Optional[date] = Query(
        default=None, description="End date for analytics"
    ),
    db_queries: DatabaseQueries = Depends(get_db_queries),
):
    """
    Get detailed analytics for a specific user.

    Returns comprehensive user analytics including:
    - Purchase patterns and spending behavior
    - Favorite categories and authors
    - Transaction frequency and timing
    - Customer lifetime value metrics
    """
    try:
        # Check if user exists
        user_info = await db_queries.get_user_by_id(user_id)
        if not user_info:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")

        # Get analytics
        params = UserHistoryQueryParams(
            pagination=PaginationParams(limit=1000, offset=0),
            date_range=DateRangeParams(start_date=start_date, end_date=end_date),
            include_analytics=True,
        )

        user_history_service = UserHistoryService(db_queries)
        _, analytics, _ = await user_history_service.get_user_history(user_id, params)

        return {
            "user_id": user_id,
            "user_info": {
                "name": user_info["name"],
                "email": user_info["email"],
                "signup_date": user_info["signup_date"],
                "user_segment": user_info["user_segment"],
            },
            "analytics": analytics,
        }

    except Exception as e:
        logger.error(f"Error in get_user_analytics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/categories", response_model=List[CategoryPerformance])
async def get_category_performance(
    start_date: Optional[date] = Query(
        default=None, description="Start date for analysis"
    ),
    end_date: Optional[date] = Query(default=None, description="End date for analysis"),
    db_queries: DatabaseQueries = Depends(get_db_queries),
):
    """
    Get comprehensive category performance analysis.

    Returns detailed metrics for each book category including:
    - Total revenue and sales counts
    - Customer engagement metrics
    - Market share analysis
    - Average pricing information
    """
    try:
        analytics_service = AnalyticsService(db_queries)
        category_performance = await analytics_service.get_category_performance(
            start_date, end_date
        )

        return category_performance

    except Exception as e:
        logger.error(f"Error in get_category_performance: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/customer-segments", response_model=List[CustomerSegment])
async def get_customer_segments(db_queries: DatabaseQueries = Depends(get_db_queries)):
    """
    Get customer segment analysis.

    Returns detailed analysis of customer segments including:
    - Segment sizes and revenue contribution
    - Average order values and lifetime values
    - Retention rates and engagement metrics
    """
    try:
        analytics_service = AnalyticsService(db_queries)
        customer_segments = await analytics_service.get_customer_segments()

        return customer_segments

    except Exception as e:
        logger.error(f"Error in get_customer_segments: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/comprehensive")
async def get_comprehensive_analytics(
    start_date: Optional[date] = Query(
        default=None, description="Start date for analysis"
    ),
    end_date: Optional[date] = Query(default=None, description="End date for analysis"),
    db_queries: DatabaseQueries = Depends(get_db_queries),
):
    """
    Get comprehensive analytics dashboard data.

    Returns a complete analytics overview including:
    - Overall platform metrics
    - Category performance analysis
    - Customer segment analysis
    - Top performers across different metrics
    - Trend analysis and insights
    """
    try:
        # Get overview
        overview = await db_queries.get_analytics_overview()

        # Get category performance
        analytics_service = AnalyticsService(db_queries)
        category_performance = await analytics_service.get_category_performance(
            start_date, end_date
        )

        # Get customer segments
        customer_segments = await analytics_service.get_customer_segments()

        # Get top books by different metrics
        top_books_service = TopBooksService(db_queries)

        top_by_revenue = await top_books_service.get_top_books(
            TopBooksQueryParams(limit=5, metric="revenue")
        )

        top_by_sales = await top_books_service.get_top_books(
            TopBooksQueryParams(limit=5, metric="sales_count")
        )

        return ComprehensiveAnalytics(
            overview=overview,
            category_performance=category_performance,
            customer_segments=customer_segments,
            trends=[],  # Would implement trend analysis
            top_performers={
                "by_revenue": top_by_revenue,
                "by_sales_count": top_by_sales,
            },
        )

    except Exception as e:
        logger.error(f"Error in get_comprehensive_analytics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
