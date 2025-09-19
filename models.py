from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class UserResponse(BaseModel):
    id: int
    name: str
    email: str
    location: str
    signup_date: date
    social_security_number: str


class TransactionResponse(BaseModel):
    transaction_id: int
    user_id: int
    book_id: int
    amount: float
    timestamp: datetime


class BookResponse(BaseModel):
    book_id: int
    title: str
    category: str
    base_price: float
    author: str
    isbn: str
    publication_year: int
    pages: int
    publisher: str


class DailySalesSummary(BaseModel):
    date: date
    total_revenue: float
    transaction_count: int
    active_users: int
    average_transaction_value: float


class TopBooksSummary(BaseModel):
    book_id: int
    title: str
    category: str
    author: str
    total_sales: int
    total_revenue: float
    average_price: float


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    database_status: Optional[str] = None
    uptime_seconds: Optional[float] = None


class AnalyticsOverview(BaseModel):
    total_users: int
    total_transactions: int
    total_revenue: float
    total_books: int
    average_transaction_value: float
    top_category: str
    most_active_user: int


class UserBehaviorAnalytics(BaseModel):
    user_segments: Dict[str, int]
    purchase_patterns: Dict[str, Any]
    retention_metrics: Dict[str, float]
    geographic_distribution: Dict[str, int]
