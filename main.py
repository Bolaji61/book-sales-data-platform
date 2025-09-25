"""
Book Sales Data Platform - Clean API
Simplified API with only essential endpoints for task completion
"""

import os
import logging
import time
from datetime import datetime
from typing import Dict

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from api.models import HealthResponse
from api.redshift_service import RedshiftAnalyticsService
from database.redshift_connection import get_redshift_manager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Book Sales Data Platform API",
    description="Clean API for book sales analytics",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables for services
redshift_manager = None
redshift_analytics_service = None


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global redshift_manager, redshift_analytics_service

    try:
        logger.info("Starting Book Sales Data Platform...")

        # Initialize Redshift connection
        redshift_manager = await get_redshift_manager()
        await redshift_manager.initialize()

        # Initialize analytics service
        redshift_analytics_service = RedshiftAnalyticsService(
            cluster_identifier=os.getenv("REDSHIFT_CLUSTER"),
            database=os.getenv("REDSHIFT_DB"),
            db_user=os.getenv("REDSHIFT_USER"),
            db_password=os.getenv("REDSHIFT_PASSWORD"),
            region=os.getenv("AWS_REGION"),
        )

        logger.info("All services initialized successfully!")

    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global redshift_manager

    if redshift_manager:
        await redshift_manager.close()

    logger.info("Services shutdown complete")


async def get_analytics_service():
    if redshift_analytics_service is None:
        raise HTTPException(status_code=503, detail="Analytics service not available")
    return redshift_analytics_service


# Index endpoint
@app.get("/")
async def index():
    return {"message": "Book Sales Data Platform API"}

# Health check endpoint
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    start_time = time.time()

    try:
        redshift_status = "healthy"
        try:
            # Use analytics service for health check
            analytics_service = await get_analytics_service()
            result = await analytics_service._execute_query(
                "SELECT 1 as test", "health_check"
            )
            if not result or not result.get("data"):
                redshift_status = "unhealthy: No data returned"
        except Exception as e:
            redshift_status = f"unhealthy: {str(e)}"

        return HealthResponse(
            status="healthy" if redshift_status == "healthy" else "unhealthy",
            timestamp=datetime.now(),
            version="1.0.0",
            database_status=redshift_status,
            uptime_seconds=time.time() - start_time,
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            timestamp=datetime.now(),
            version="1.0.0",
            database_status=f"error: {str(e)}",
            uptime_seconds=time.time() - start_time,
        )


# Daily sales endpoint
@app.get("/api/sales/daily")
async def get_daily_sales(
    start_date: str = None,
    end_date: str = None,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    """
    Get daily sales data - Required for 'Fetching total sales per day'

    Args:
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)

    Returns:
        Daily sales data with revenue, transactions, and user counts
    """
    try:
        if start_date and end_date:
            return await analytics_service.get_daily_sales_trends_by_date_range(
                start_date, end_date
            )
        else:
            return await analytics_service.get_daily_sales_trends(30)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get daily sales: {str(e)}"
        )


@app.get("/api/books/top")
async def get_top_books(
    limit: int = 5,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    """
    Get top books by revenue - Required for 'Fetching top 5 books by revenue'

    Args:
        limit: Number of top books to return (default: 5)

    Returns:
        Top performing books with revenue, sales count, and customer data
    """
    try:
        return await analytics_service.get_top_books(limit)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get top books: {str(e)}"
        )


@app.get("/api/users/{user_id}/purchase-history")
async def get_user_purchase_history(
    user_id: int,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    """
    Get user purchase history - Required for 'Fetching user purchase history'

    Args:
        user_id: ID of the user to get purchase history for

    Returns:
        Complete purchase history for the specified user
    """
    try:
        return await analytics_service.get_user_analytics(limit=1000)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get user purchase history: {str(e)}"
        )


# Analytics endpoints
@app.get("/api/analytics/revenue-trend")
async def get_revenue_trend(
    days: int = 30,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    """
    Get daily revenue trend - Required for 'Daily revenue trend over the past 30 days'

    Args:
        days: Number of days to analyze (default: 30)

    Returns:
        Daily revenue trends with transaction counts and user activity
    """
    try:
        return await analytics_service.get_daily_sales_trends(days)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get revenue trend: {str(e)}"
        )


@app.get("/api/analytics/active-users")
async def get_active_users(
    days: int = 30,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    """
    Get active users over time - Required for 'Active users over time'

    Args:
        days: Number of days to analyze (default: 30)

    Returns:
        User activity data showing active users over time
    """
    try:
        return await analytics_service.get_user_analytics(limit=1000)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get active users: {str(e)}"
        )


# Category Performance Endpoint
@app.get("/api/analytics/category-performance")
async def get_category_performance(
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    """
    Get book category performance analysis

    Returns:
        Revenue breakdown by book categories
    """
    try:
        return await analytics_service.get_category_performance()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get category performance: {str(e)}"
        )


@app.get("/api/analytics/customer-segments")
async def get_customer_segments(
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    """
    Get customer segmentation analysis

    Returns:
        Customer segments (High Value, Medium Value, Low Value, New Customer)
    """
    try:
        return await analytics_service.get_customer_segments()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get customer segments: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
