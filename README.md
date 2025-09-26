# Book Sales Data Platform

A simple data platform and analytics dashboard for book sales data. Shows daily revenue trends, top-selling books, and customer insights.

## Project Summary

- Loads book sales data into Amazon Redshift
- Provides a REST API for analytics queries
- Shows interactive charts and metrics in a web dashboard
- Tracks daily sales, popular books, and customer behavior

## Quick Start

### Option 1: Docker (Recommended)

1. **Setup environment:**
   ```bash
   cp env.example .env
   # Edit .env with your AWS credentials
   ```

2. **Start everything:**
   ```bash
   docker-compose up --build
   ```

3. **Access the dashboard:**
   - API: http://localhost:8000
   - Dashboard: http://localhost:8501
   - API docs: http://localhost:8000/docs

### Option 2: Manual Setup

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   cp env.example .env
   # Add your AWS and Redshift details
   ```

3. **Upload data to S3:**
   ```bash
   python etl/upload_data_to_s3.py your-s3-bucket-name
   ```

4. **Setup Redshift:**
   ```bash
   ./deploy/redshift-setup.sh
   ```

5. **Run the API:**
   ```bash
   python main.py
   ```

6. **Run the dashboard:**
   ```bash
   cd streamlit
   streamlit run app.py
   ```

## What You Need

- Python 3.9 or newer
- AWS account with S3 and Redshift access
- Docker

## Data Files

The platform uses three CSV files:
- `data/users.csv` - Customer information
- `data/books.csv` - Book catalog with prices
- `data/transactions.csv` - Purchase records

## API Endpoints

- `GET /health` - Check if everything is working
- `GET /api/sales/daily` - Daily sales totals
- `GET /api/books/top` - Top 5 books by revenue
- `GET /api/users/{user_id}/purchase-history` - User's buying history
- `GET /api/analytics/revenue-trend` - Revenue trends over time
- `GET /api/analytics/active-users` - User activity over time
- `GET /api/analytics/category-performance` - Sales by book category
- `GET /api/analytics/customer-segments` - Customer groups (high-value, etc.)

## Dashboard Features

The Streamlit dashboard shows:
- **Revenue Trends** - Daily sales with date range picker
- **Top Books** - Best sellers by revenue
- **User Analytics** - Customer segments and behavior
- **Category Analysis** - Performance by book category
- **System Status** - Health check for API and database

## Project Structure

```
├── api/                    # FastAPI backend
│   ├── endpoints.py        # API route handlers
│   ├── models.py          # Data models
│   ├── redshift_service.py # Database queries
│   └── services.py        # Business logic
├── etl/                   # Data processing
│   ├── data_processor.py  # Main ETL pipeline
│   └── upload_data_to_s3.py # S3 upload utility
├── database/              # Database setup
│   ├── init_db.py         # Create tables
│   ├── schema.sql         # Table definitions
│   └── queries.py         # Common queries
├── streamlit/             # Web dashboard
│   └── app.py            # Dashboard app
├── deploy/                # Deployment scripts
├── main.py               # API entry point
└── requirements.txt      # Python packages
```

