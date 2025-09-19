"""
API models for Book Sales Data Platform
"""

from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"


class TimeRange(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"


class MetricType(str, Enum):
    REVENUE = "revenue"
    SALES_COUNT = "sales_count"
    CUSTOMERS = "customers"
    BOOKS_SOLD = "books_sold"


class PaginationParams(BaseModel):
    """Pagination parameters"""

    limit: int = Field(
        default=100, ge=1, le=1000, description="Number of items to return"
    )
    offset: int = Field(default=0, ge=0, description="Number of items to skip")


class DateRangeParams(BaseModel):
    """Date range parameters"""

    start_date: Optional[date] = Field(
        default=None, description="Start date for filtering"
    )
    end_date: Optional[date] = Field(default=None, description="End date for filtering")

    @validator("end_date")
    def validate_date_range(cls, v, values):
        if (
            v
            and "start_date" in values
            and values["start_date"]
            and v < values["start_date"]
        ):
            raise ValueError("end_date must be after start_date")
        return v


class SalesQueryParams(BaseModel):
    """Sales query parameters"""

    pagination: PaginationParams = Field(default_factory=PaginationParams)
    date_range: DateRangeParams = Field(default_factory=DateRangeParams)
    category: Optional[str] = Field(default=None, description="Filter by book category")
    user_segment: Optional[str] = Field(
        default=None, description="Filter by user segment"
    )


class TopBooksQueryParams(BaseModel):
    """Top books query parameters"""

    limit: int = Field(
        default=5, ge=1, le=100, description="Number of top books to return"
    )
    metric: MetricType = Field(
        default=MetricType.REVENUE, description="Metric to rank by"
    )
    category: Optional[str] = Field(default=None, description="Filter by book category")
    time_range: Optional[TimeRange] = Field(
        default=None, description="Time range for analysis"
    )


class UserHistoryQueryParams(BaseModel):
    """User history query parameters"""

    pagination: PaginationParams = Field(default_factory=PaginationParams)
    date_range: DateRangeParams = Field(default_factory=DateRangeParams)
    include_analytics: bool = Field(default=True, description="Include user analytics")


# response models
class SalesData(BaseModel):
    """Daily sales data"""

    date: date
    total_revenue: float = Field(description="Total revenue for the day")
    transaction_count: int = Field(description="Number of transactions")
    unique_customers: int = Field(description="Number of unique customers")
    average_transaction_value: float = Field(description="Average transaction value")
    total_books_sold: int = Field(description="Total books sold")


class TopBookData(BaseModel):
    """Top book performance data"""

    book_id: int
    title: str
    author: str
    category: str
    total_revenue: float
    total_sales: int
    average_price: float
    unique_customers: int
    rank: int = Field(description="Rank based on selected metric")


class UserPurchaseData(BaseModel):
    """Individual purchase data"""

    transaction_id: int
    transaction_date: datetime
    book_title: str
    book_category: str
    book_author: str
    amount: float
    quantity: int


class UserAnalytics(BaseModel):
    """User analytics summary"""

    total_transactions: int
    total_spent: float
    average_transaction_value: float
    first_purchase_date: Optional[datetime]
    last_purchase_date: Optional[datetime]
    unique_books_purchased: int
    favorite_category: Optional[str]
    user_segment: str


class UserHistoryResponse(BaseModel):
    """Complete user history response"""

    user_id: int
    user_name: str
    user_email: str
    purchases: List[UserPurchaseData]
    analytics: Optional[UserAnalytics]
    pagination: Dict[str, Any]


class SalesResponse(BaseModel):
    """Sales data response"""

    data: List[SalesData]
    total_records: int
    pagination: Dict[str, Any]
    summary: Dict[str, Any]


class TopBooksResponse(BaseModel):
    """Top books response"""

    data: List[TopBookData]
    metric_used: str
    time_range: Optional[str]
    category_filter: Optional[str]


class ErrorResponse(BaseModel):
    """Error response model"""

    error: str
    detail: str
    timestamp: datetime = Field(default_factory=datetime.now)


class HealthResponse(BaseModel):
    """Health check response"""

    status: str
    timestamp: datetime
    version: str
    database_status: str
    uptime_seconds: float


class APIStatsResponse(BaseModel):
    """API statistics response"""

    total_requests: int
    average_response_time: float
    error_rate: float
    most_popular_endpoints: List[Dict[str, Any]]
    database_queries_performed: int


# analytics models
class CategoryPerformance(BaseModel):
    """Category performance data"""

    category: str
    total_revenue: float
    total_sales: int
    unique_customers: int
    average_price: float
    market_share: float = Field(description="Percentage of total revenue")


class TimeSeriesData(BaseModel):
    """Time series data point"""

    date: date
    value: float
    label: str


class TrendAnalysis(BaseModel):
    """Trend analysis data"""

    metric: str
    time_range: str
    data_points: List[TimeSeriesData]
    trend_direction: str = Field(description="up, down, or stable")
    percentage_change: float
    confidence_level: float


class CustomerSegment(BaseModel):
    """Customer segment data"""

    segment: str
    customer_count: int
    total_revenue: float
    average_order_value: float
    retention_rate: float
    lifetime_value: float


class ComprehensiveAnalytics(BaseModel):
    """Comprehensive analytics response"""

    overview: Dict[str, Any]
    category_performance: List[CategoryPerformance]
    customer_segments: List[CustomerSegment]
    trends: List[TrendAnalysis]
    top_performers: Dict[str, List[TopBookData]]
    generated_at: datetime = Field(default_factory=datetime.now)
