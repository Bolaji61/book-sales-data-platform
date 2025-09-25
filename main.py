"""
Book Sales Analytics Platform - Main Application
"""

import os
import time
from contextlib import asynccontextmanager
from datetime import datetime

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from api.redshift_service import RedshiftAnalyticsService
from database.redshift_connection import close_redshift, get_redshift_manager
from etl.data_processor import DataProcessor
from logger import get_logger, log_error, log_info, log_start, log_success
from models import HealthResponse

load_dotenv()

redshift_manager = None
data_processor = None
redshift_analytics_service = None
logger = get_logger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global redshift_manager, data_processor, redshift_analytics_service

    try:
        log_start("Starting Book Sales Platform", logger)

        redshift_manager = await get_redshift_manager()
        log_success("Redshift connection established", logger)

        data_processor = DataProcessor()
        await data_processor.initialize()
        log_success("ETL: cleaned local CSVs", logger)

        redshift_analytics_service = RedshiftAnalyticsService(
            cluster_identifier=os.getenv(
                "REDSHIFT_CLUSTER", "book-sales-platform-redshift"
            ),
            database=os.getenv("REDSHIFT_DB", "book_sales"),
            db_user=os.getenv("REDSHIFT_USER", "admin"),
            db_password=os.getenv("REDSHIFT_PASSWORD", ""),
            port=int(os.getenv("REDSHIFT_PORT", "5439")),
            region=os.getenv("AWS_REGION", "us-east-2"),
        )
        log_success("Analytics service initialized", logger)

        log_success("Book Sales Platform is ready!", logger)

    except Exception as e:
        log_error(f"Failed to initialize application: {e}", logger)
        raise

    yield

    try:
        log_info("Shutting down Book Sales Platform...", logger)

        if redshift_manager:
            await close_redshift()
            log_success("Redshift connection closed", logger)

        log_success("Shutdown complete", logger)

    except Exception as e:
        log_error(f"Error during shutdown: {e}", logger)


app = FastAPI(
    title="Book Sales Platform",
    description="Analytics platform for book sales data using Amazon Redshift",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simple monitoring - no complex middleware needed for take-home assignment


async def get_redshift():
    if redshift_manager is None:
        raise HTTPException(status_code=503, detail="Redshift not available")
    return redshift_manager


async def get_analytics_service():
    if redshift_analytics_service is None:
        raise HTTPException(status_code=503, detail="Analytics service not available")
    return redshift_analytics_service


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    start_time = time.time()

    try:
        redshift_status = "healthy"
        try:
            # Use analytics service for health check
            analytics_service = await get_analytics_service()
            result = await analytics_service._execute_query("SELECT 1 as test", "health_check")
            if not result or not result.get("data"):
                redshift_status = "unhealthy: No data returned"
        except Exception as e:
            redshift_status = f"unhealthy: {str(e)}"

        return HealthResponse(
            status="healthy" if redshift_status == "healthy" else "unhealthy",
            timestamp=datetime.now(),
            version="2.0.0",
            database_status=redshift_status,
            uptime_seconds=time.time() - start_time,
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            timestamp=datetime.now(),
            version="2.0.0",
            database_status=f"error: {str(e)}",
            uptime_seconds=time.time() - start_time,
        )


@app.get("/analytics/daily-sales-trends")
async def get_daily_sales_trends(
    days: int = 30,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_daily_sales_trends(days)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get daily sales trends: {str(e)}"
        )


@app.get("/analytics/top-books")
async def get_top_books(
    limit: int = 10,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_top_books(limit)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get top books: {str(e)}"
        )


@app.get("/analytics/user-analytics")
async def get_user_analytics(
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_user_analytics()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get user analytics: {str(e)}"
        )


@app.get("/debug/data-status")
async def debug_data_status(
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    """Debug endpoint to check data status in Redshift tables"""
    try:
        return await analytics_service.debug_data_status()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get debug data: {str(e)}"
        )


@app.get("/analytics/daily-sales-trends-range")
async def get_daily_sales_trends_by_range(
    start_date: str,
    end_date: str,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    """Get daily sales trends for a specific date range (format: YYYY-MM-DD)"""
    try:
        return await analytics_service.get_daily_sales_trends_by_date_range(start_date, end_date)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get daily sales trends: {str(e)}"
        )


@app.get("/analytics/category-performance")
async def get_category_performance(
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_category_performance()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get category performance: {str(e)}"
        )


@app.get("/analytics/customer-segments")
async def get_customer_segments(
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_customer_segments()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get customer segments: {str(e)}"
        )


@app.get("/analytics/monthly-trends")
async def get_monthly_trends(
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_monthly_trends()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get monthly trends: {str(e)}"
        )


@app.get("/analytics/sales-summary")
async def get_sales_summary_endpoint(
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_sales_summary()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get sales summary: {str(e)}"
        )


@app.get("/api/v1/sales/daily")
async def get_daily_sales_v1(
    limit: int = 100,
    start_date: str = None,
    end_date: str = None,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_daily_sales_trends(30)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get daily sales: {str(e)}"
        )


@app.get("/api/v1/books/top")
async def get_top_books_v1(
    limit: int = 10,
    metric: str = "revenue",
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_top_books(limit)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get top books: {str(e)}"
        )


@app.get("/api/v1/analytics/categories")
async def get_category_performance_v1(
    start_date: str = None,
    end_date: str = None,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_category_performance()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get category performance: {str(e)}"
        )


@app.get("/api/v1/analytics/customer-segments")
async def get_customer_segments_v1(
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_user_analytics()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get customer segments: {str(e)}"
        )


@app.get("/api/v1/analytics/comprehensive")
async def get_comprehensive_analytics_v1(
    start_date: str = None,
    end_date: str = None,
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        sales_summary = await analytics_service.get_sales_summary()
        top_books = await analytics_service.get_top_books(5)
        user_analytics = await analytics_service.get_user_analytics()
        category_performance = await analytics_service.get_category_performance()

        return {
            "overview": sales_summary,
            "top_performers": {"by_revenue": top_books},
            "category_performance": category_performance,
            "customer_segments": user_analytics,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get comprehensive analytics: {str(e)}"
        )


@app.get("/data-quality/summary")
async def get_data_quality_summary(
    analytics_service: RedshiftAnalyticsService = Depends(get_analytics_service),
):
    try:
        return await analytics_service.get_data_quality_summary()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get data quality summary: {str(e)}"
        )


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    # Simple error logging - no complex alerting for take-home assignment
    log_error(f"Unhandled exception: {str(exc)}", logger)
    return HTTPException(status_code=500, detail="Internal server error")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
