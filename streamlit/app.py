"""
Book Sales Data Platform - Streamlit Dashboard
Interactive analytics dashboard for book sales data
"""

import os
from datetime import date, timedelta
from typing import Any, Dict, List

import pandas as pd
import plotly.graph_objects as go
import requests

import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Book Sales Analytics Dashboard",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling
st.markdown(
    """
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #1f77b4;
    }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
</style>
""",
    unsafe_allow_html=True,
)


class BookSalesAPI:
    """Client for Book Sales API"""

    def __init__(self, base_url: str = None):
        self.base_url = base_url or os.getenv("API_BASE_URL", "http://api:8000")

    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make API request with error handling"""
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            st.error(f"API request failed: {e}")
            return {}

    def get_daily_sales(
        self, start_date: date = None, end_date: date = None, limit: int = 100
    ) -> Dict:
        """Get daily sales data"""
        if start_date and end_date:
            # Use the new date range endpoint
            params = {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            }
            return self._make_request("/analytics/daily-sales-trends-range", params)
        else:
            # Use the original endpoint for backward compatibility
            params = {
                "limit": limit,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
            }
            params = {k: v for k, v in params.items() if v is not None}
            return self._make_request("/api/v1/sales/daily", params)

    def get_top_books(self, limit: int = 10, metric: str = "revenue") -> Dict:
        """Get top books"""
        params = {"limit": limit}
        return self._make_request("/analytics/top-books", params)

    def get_category_performance(
        self, start_date: date = None, end_date: date = None
    ) -> List[Dict]:
        """Get category performance"""
        params = {
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        }
        params = {k: v for k, v in params.items() if v is not None}
        return self._make_request("/analytics/category-performance", params)

    def get_customer_segments(self) -> List[Dict]:
        """Get customer segments"""
        return self._make_request("/analytics/customer-segments")

    def get_comprehensive_analytics(
        self, start_date: date = None, end_date: date = None
    ) -> Dict:
        """Get comprehensive analytics"""
        params = {
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        }
        params = {k: v for k, v in params.items() if v is not None}
        return self._make_request("/api/v1/analytics/comprehensive", params)

    def health_check(self) -> Dict:
        """Check API health"""
        return self._make_request("/health")


def create_revenue_trend_chart(sales_data: List[Dict], start_date: str = None, end_date: str = None) -> go.Figure:
    """Create daily revenue trend chart"""
    if not sales_data:
        return go.Figure()

    try:
        df = pd.DataFrame(sales_data)
        df["date"] = pd.to_datetime(df["date"])

        # Ensure numeric columns are properly formatted
        df["total_revenue"] = pd.to_numeric(
            df["total_revenue"], errors="coerce"
        ).fillna(0)
        
        # Handle both transaction_count and total_transactions columns
        if "transaction_count" in df.columns:
            df["transaction_count"] = pd.to_numeric(
                df["transaction_count"], errors="coerce"
            ).fillna(0)
        elif "total_transactions" in df.columns:
            df["transaction_count"] = pd.to_numeric(
                df["total_transactions"], errors="coerce"
            ).fillna(0)
        else:
            df["transaction_count"] = 0
        # Handle both unique_customers and total_customers columns
        if "unique_customers" in df.columns:
            df["unique_customers"] = pd.to_numeric(
                df["unique_customers"], errors="coerce"
            ).fillna(0)
        elif "total_customers" in df.columns:
            df["unique_customers"] = pd.to_numeric(
                df["total_customers"], errors="coerce"
            ).fillna(0)
        else:
            df["unique_customers"] = 0
    except Exception as e:
        st.error(f"Error processing sales data: {e}")
        return go.Figure()

    fig = go.Figure()

    # Add revenue line
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["total_revenue"],
            mode="lines+markers",
            name="Daily Revenue",
            line=dict(color="#1f77b4", width=3),
            marker=dict(size=8),
            hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.2f}<extra></extra>",
        )
    )

    # Add transaction count as secondary y-axis
    fig2 = go.Scatter(
        x=df["date"],
        y=df["transaction_count"],
        mode="lines",
        name="Transactions",
        line=dict(color="#ff7f0e", width=2),
        yaxis="y2",
        hovertemplate="<b>%{x}</b><br>Transactions: %{y}<extra></extra>",
    )
    fig.add_trace(fig2)

    # Create dynamic title based on date range
    if start_date and end_date:
        title_text = f"Daily Revenue Trend ({start_date} to {end_date})"
    else:
        title_text = "Daily Revenue Trend"
    
    # Update layout
    fig.update_layout(
        title={
            "text": title_text,
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 20},
        },
        xaxis_title="Date",
        yaxis_title="Revenue ($)",
        yaxis2=dict(title="Number of Transactions", overlaying="y", side="right"),
        hovermode="x unified",
        template="plotly_white",
        height=500,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def create_top_books_chart(top_books: List[Dict]) -> go.Figure:
    """Create top books visualization"""
    if not top_books:
        return go.Figure()

    try:
        df = pd.DataFrame(top_books)

        # Ensure numeric columns are properly formatted
        df["total_revenue"] = pd.to_numeric(
            df["total_revenue"], errors="coerce"
        ).fillna(0)
        df["total_sales"] = pd.to_numeric(df["total_sales"], errors="coerce").fillna(0)
        # Handle both unique_customers and total_customers columns
        if "unique_customers" in df.columns:
            df["unique_customers"] = pd.to_numeric(
                df["unique_customers"], errors="coerce"
            ).fillna(0)
        elif "total_customers" in df.columns:
            df["unique_customers"] = pd.to_numeric(
                df["total_customers"], errors="coerce"
            ).fillna(0)
        else:
            df["unique_customers"] = 0
    except Exception as e:
        st.error(f"Error processing top books data: {e}")
        return go.Figure()

    fig = go.Figure()

    # Create horizontal bar chart
    fig.add_trace(
        go.Bar(
            y=df["title"],
            x=df["total_revenue"],
            orientation="h",
            marker=dict(
                color=df["total_revenue"],
                colorscale="Blues",
                showscale=True,
                colorbar=dict(title="Revenue ($)"),
            ),
            text=[f"${x:,.0f}" for x in df["total_revenue"]],
            textposition="inside",
            hovertemplate="<b>%{y}</b><br>Revenue: $%{x:,.2f}<br>Sales: %{customdata[0]}<br>Customers: %{customdata[1]}<extra></extra>",
            customdata=list(zip(df["total_sales"], df["unique_customers"])),
        )
    )

    fig.update_layout(
        title={
            "text": "Top Books by Revenue",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 20},
        },
        xaxis_title="Revenue ($)",
        yaxis_title="Book Title",
        template="plotly_white",
        height=600,
        yaxis=dict(autorange="reversed"),
    )

    return fig


def create_active_users_chart(sales_data: List[Dict], start_date: str = None, end_date: str = None) -> go.Figure:
    """Create active users over time chart"""
    if not sales_data:
        return go.Figure()

    try:
        df = pd.DataFrame(sales_data)
        df["date"] = pd.to_datetime(df["date"])

        # Ensure numeric columns are properly formatted
        # Handle both unique_customers and total_customers columns
        if "unique_customers" in df.columns:
            df["unique_customers"] = pd.to_numeric(
                df["unique_customers"], errors="coerce"
            ).fillna(0)
        elif "total_customers" in df.columns:
            df["unique_customers"] = pd.to_numeric(
                df["total_customers"], errors="coerce"
            ).fillna(0)
        else:
            df["unique_customers"] = 0
            
        # Handle both transaction_count and total_transactions columns
        if "transaction_count" in df.columns:
            df["transaction_count"] = pd.to_numeric(
                df["transaction_count"], errors="coerce"
            ).fillna(0)
        elif "total_transactions" in df.columns:
            df["transaction_count"] = pd.to_numeric(
                df["total_transactions"], errors="coerce"
            ).fillna(0)
        else:
            df["transaction_count"] = 0
    except Exception as e:
        st.error(f"Error processing user analytics data: {e}")
        return go.Figure()

    fig = go.Figure()

    # Add unique customers line
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["unique_customers"],
            mode="lines+markers",
            name="Active Users",
            line=dict(color="#2ca02c", width=3),
            marker=dict(size=8),
            hovertemplate="<b>%{x}</b><br>Active Users: %{y}<extra></extra>",
        )
    )

    # Add transaction count for context
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["transaction_count"],
            mode="lines",
            name="Total Transactions",
            line=dict(color="#d62728", width=2),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Transactions: %{y}<extra></extra>",
        )
    )

    # Create dynamic title based on date range
    if start_date and end_date:
        title_text = f"Active Users Over Time ({start_date} to {end_date})"
    else:
        title_text = "Active Users Over Time"
    
    fig.update_layout(
        title={
            "text": title_text,
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 20},
        },
        xaxis_title="Date",
        yaxis_title="Active Users",
        yaxis2=dict(title="Total Transactions", overlaying="y", side="right"),
        hovermode="x unified",
        template="plotly_white",
        height=500,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def create_category_performance_chart(category_data: List[Dict]) -> go.Figure:
    """Create category performance pie chart"""
    if not category_data:
        return go.Figure()

    try:
        df = pd.DataFrame(category_data)

        # Ensure numeric columns are properly formatted
        df["total_revenue"] = pd.to_numeric(
            df["total_revenue"], errors="coerce"
        ).fillna(0)
    except Exception as e:
        st.error(f"Error processing category data: {e}")
        return go.Figure()

    fig = go.Figure(
        data=[
            go.Pie(
                labels=df["category"],
                values=df["total_revenue"],
                hole=0.3,
                textinfo="label+percent",
                textposition="outside",
                hovertemplate="<b>%{label}</b><br>Revenue: $%{value:,.2f}<br>Market Share: %{percent}<extra></extra>",
            )
        ]
    )

    fig.update_layout(
        title={
            "text": "Revenue by Category",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 20},
        },
        template="plotly_white",
        height=500,
        showlegend=True,
    )

    return fig


def create_customer_segments_chart(segments_data: List[Dict]) -> go.Figure:
    """Create customer segments visualization"""
    if not segments_data:
        return go.Figure()

    try:
        df = pd.DataFrame(segments_data)

        # Ensure numeric columns are properly formatted
        df["customer_count"] = pd.to_numeric(
            df["customer_count"], errors="coerce"
        ).fillna(0)
        df["total_revenue"] = pd.to_numeric(
            df["total_revenue"], errors="coerce"
        ).fillna(0)
        
        # Handle both average_order_value and avg_spent columns
        if "average_order_value" in df.columns:
            df["average_order_value"] = pd.to_numeric(
                df["average_order_value"], errors="coerce"
            ).fillna(0)
        elif "avg_spent" in df.columns:
            df["average_order_value"] = pd.to_numeric(
                df["avg_spent"], errors="coerce"
            ).fillna(0)
        else:
            df["average_order_value"] = 0
    except Exception as e:
        st.error(f"Error processing customer segments data: {e}")
        return go.Figure()

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df["segment"],
            y=df["customer_count"],
            name="Customer Count",
            marker=dict(color="#1f77b4"),
            text=df["customer_count"],
            textposition="auto",
            hovertemplate="<b>%{x}</b><br>Customers: %{y}<br>Revenue: $%{customdata[0]:,.2f}<br>Avg Order Value: $%{customdata[1]:,.2f}<extra></extra>",
            customdata=list(zip(df["total_revenue"], df["average_order_value"])),
        )
    )

    fig.update_layout(
        title={
            "text": "Customer Segments",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 20},
        },
        xaxis_title="Customer Segment",
        yaxis_title="Number of Customers",
        template="plotly_white",
        height=400,
    )

    return fig


def main():
    """Main Streamlit application"""

    # Header
    st.markdown(
        '<h1 class="main-header">📚 Book Sales Analytics Dashboard</h1>',
        unsafe_allow_html=True,
    )

    # Initialize API client
    api = BookSalesAPI()

    # Sidebar controls
    st.sidebar.title("Dashboard Controls")

    # Date range selector
    st.sidebar.subheader("Date Range")
    end_date = date.today()
    start_date = end_date - timedelta(days=30)

    col1, col2 = st.sidebar.columns(2)
    with col1:
        selected_start = st.date_input("Start Date", start_date)
    with col2:
        selected_end = st.date_input("End Date", end_date)

    # Metric selector
    st.sidebar.subheader("Metrics")
    top_books_metric = st.sidebar.selectbox(
        "Top Books Metric", ["revenue", "sales_count", "customers"], index=0
    )

    top_books_limit = st.sidebar.slider("Number of Top Books", 5, 20, 10)

    # Refresh button
    if st.sidebar.button("Refresh Data", type="primary"):
        st.rerun()

    # API Health Check
    st.sidebar.subheader("System Status")
    with st.spinner("Checking API health..."):
        health = api.health_check()
        if health:
            st.sidebar.success("API Connected")
            st.sidebar.info(f"Status: {health.get('status', 'Unknown')}")
            st.sidebar.info(f"Database: {health.get('database_status', 'Unknown')}")
        else:
            st.sidebar.error("API Disconnected")
            st.stop()

    # Main dashboard content
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Revenue Trends", "Top Books", "User Analytics", "Category Analysis"]
    )

    with tab1:
        st.header("Revenue Trends")

        # Load sales data
        with st.spinner("Loading sales data..."):
            sales_response = api.get_daily_sales(selected_start, selected_end)

        if sales_response and "data" in sales_response:
            sales_data = sales_response["data"]
            
            # Calculate summary metrics from daily data
            if sales_data:
                df_sales = pd.DataFrame(sales_data)
                
                # Convert numeric columns
                df_sales["total_revenue"] = pd.to_numeric(df_sales["total_revenue"], errors="coerce").fillna(0)
                df_sales["total_transactions"] = pd.to_numeric(df_sales["total_transactions"], errors="coerce").fillna(0)
                
                # Calculate summary metrics
                total_revenue = df_sales["total_revenue"].sum()
                total_transactions = df_sales["total_transactions"].sum()
                avg_transaction_value = total_revenue / total_transactions if total_transactions > 0 else 0
                days_with_sales = len(df_sales[df_sales["total_transactions"] > 0])
            else:
                total_revenue = 0
                total_transactions = 0
                avg_transaction_value = 0
                days_with_sales = 0

            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Revenue", f"${total_revenue:,.2f}", delta=None)
            with col2:
                st.metric("Total Transactions", f"{total_transactions:,}", delta=None)
            with col3:
                st.metric(
                    "Avg Transaction Value",
                    f"${avg_transaction_value:,.2f}",
                    delta=None,
                )
            with col4:
                st.metric("Days with Sales", f"{days_with_sales}", delta=None)

            # Revenue trend chart
            fig_revenue = create_revenue_trend_chart(
                sales_data, 
                selected_start.isoformat(), 
                selected_end.isoformat()
            )
            st.plotly_chart(fig_revenue, use_container_width=True)

            # Data table
            if st.checkbox("Show Raw Data"):
                df_sales = pd.DataFrame(sales_data)
                st.dataframe(df_sales, use_container_width=True)
        else:
            st.error("Failed to load sales data")

    with tab2:
        st.header("📚 Top Books Analysis")

        # Load top books data
        with st.spinner("Loading top books data..."):
            top_books_response = api.get_top_books(top_books_limit, top_books_metric)

        if top_books_response and "data" in top_books_response:
            top_books = top_books_response["data"]

            # Top books chart
            fig_top_books = create_top_books_chart(top_books)
            st.plotly_chart(fig_top_books, use_container_width=True)

            # Top books table
            df_top_books = pd.DataFrame(top_books)
            st.dataframe(df_top_books, use_container_width=True)
        else:
            st.error("Failed to load top books data")

    with tab3:
        st.header("👥 User Analytics")

        # Load sales data for user analytics
        with st.spinner("Loading user analytics..."):
            sales_response = api.get_daily_sales(selected_start, selected_end)
            segments_response = api.get_customer_segments()

        if sales_response and "data" in sales_response:
            sales_data = sales_response["data"]

            # Active users chart
            fig_users = create_active_users_chart(
                sales_data, 
                selected_start.isoformat(), 
                selected_end.isoformat()
            )
            st.plotly_chart(fig_users, use_container_width=True)

            # Customer segments
            if segments_response:
                # Extract data from response if it's wrapped in a data field
                if isinstance(segments_response, dict) and "data" in segments_response:
                    segments_data = segments_response["data"]
                else:
                    segments_data = segments_response
                    
                fig_segments = create_customer_segments_chart(segments_data)
                st.plotly_chart(fig_segments, use_container_width=True)

            # User analytics summary
            if sales_data:
                df_sales = pd.DataFrame(sales_data)
                # Check if unique_customers column exists, otherwise use total_customers
                if "unique_customers" in df_sales.columns:
                    avg_daily_users = (
                        df_sales["unique_customers"].mean() if not df_sales.empty else 0
                    )
                    max_daily_users = (
                        df_sales["unique_customers"].max() if not df_sales.empty else 0
                    )
                elif "total_customers" in df_sales.columns:
                    avg_daily_users = (
                        df_sales["total_customers"].mean() if not df_sales.empty else 0
                    )
                    max_daily_users = (
                        df_sales["total_customers"].max() if not df_sales.empty else 0
                    )
                else:
                    avg_daily_users = 0
                    max_daily_users = 0

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Average Daily Active Users", f"{avg_daily_users:.0f}")
                with col2:
                    st.metric("Peak Daily Active Users", f"{max_daily_users:.0f}")
        else:
            st.error("Failed to load user analytics data")

    with tab4:
        st.header("Category Analysis")

        # Load category performance data
        with st.spinner("Loading category data..."):
            category_response = api.get_category_performance(
                selected_start, selected_end
            )

        if category_response:
            # Extract data from response if it's wrapped in a data field
            if isinstance(category_response, dict) and "data" in category_response:
                category_data = category_response["data"]
            else:
                category_data = category_response
                
            # Category performance pie chart
            fig_category = create_category_performance_chart(category_data)
            st.plotly_chart(fig_category, use_container_width=True)

            # Category performance table
            df_category = pd.DataFrame(category_data)
            st.dataframe(df_category, use_container_width=True)
        else:
            st.error("Failed to load category data")

    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            📚 Book Sales Data Platform | Built with Streamlit & Plotly
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
