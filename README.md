# Book Sales Analytics Platform

A data analytics platform for book sales data using FastAPI, Amazon Redshift, and AWS services.

## Architecture & Design Decisions

### Data Flow
1. **CSV Files** → **S3 Storage** → **Redshift Data Warehouse** → **FastAPI** → **Analytics Dashboard**

### Key Design Decisions

**Redshift:**
- Handles large datasets efficiently with columnar storage
- Built-in analytics functions and materialized views
- Integrates well with S3 for data loading

**Star Schema:**
- Simple to understand and query
- Optimized for analytics workloads
- Easy to add new dimensions

**Materialized Views:**
- Pre-computed aggregations for faster queries
- Automatically refreshed when data changes
- Reduces load on fact tables

**FastAPI:**
- Fast and modern Python web framework
- Built-in API documentation
- Easy to extend with new endpoints

## How to Run the Pipeline and API

### Prerequisites
- Python 3.9+
- AWS CLI configured
- Docker

### Docker Setup

```bash
cp env.example .env  # Edit .env with AWS and Redshift credentials 
docker-compose up --build
```

### Alternative Setup

1. **Install Dependencies**
```bash
pip install -r requirements.txt
```

2. **Configure Environment**
```bash
cp env.example .env
# Edit .env with AWS and Redshift credentials 
```

3. **Upload Data to S3 (One-time setup)**
```bash
python3 scripts/upload_data_to_s3.py s3-bucket-name
```

4. **Deploy Redshift Infrastructure**
```bash
./deploy/redshift-setup.sh
```

5. **Run the API**
```bash
python main.py
```

The API will be available at `http://localhost:8000`

### API Endpoints

- `GET /health` - Health check
- `GET /analytics/daily-sales` - Daily sales summary
- `GET /analytics/top-books` - Top performing books
- `GET /analytics/user-behavior` - User analytics
- `GET /docs` - API documentation


## Monitoring Setup

### Logging

- **Console Logging**: All operations logged to console with timestamps
- **Log Levels**: INFO, SUCCESS, WARNING, ERROR

## How to Access Analytics/Dashboard

### Streamlit Dashboard
This data platform includes a Streamlit dashboard for interactive analytics:

1. **Run Dashboard**
```bash
cd streamlit
streamlit run app.py
```

2. **Access Dashboard**
Open `http://localhost:8501` in your browser

### Dashboard Features
- **Sales Overview**: Daily sales trends and metrics
- **Top Books**: Best performing books by revenue
- **User Analytics**: Customer behavior and segments
- **Data Quality**: Data validation and cleaning results

### API Documentation
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

## Data Structure

### Input Data
- `users.csv` - User profiles (ID, name, email, location, signup date)
- `books.csv` - Book catalog (ID, title, author, category, price, ISBN)
- `transactions.csv` - Purchase records (transaction ID, user ID, book ID, amount, timestamp)

### Database Schema
- **dim_users** - User dimension table
- **dim_books** - Book dimension table  
- **fact_sales** - Sales fact table
- **Materialized Views** - Pre-aggregated analytics tables

## Environment Variables
See `env.example` file


## Project Structure

```
├── api/                    # FastAPI endpoints and services
├── etl/                    # Core Extract, Transform, Load logic
├── database/               # Database connections and queries
├── deploy/                 # Deployment scripts
├── scripts/                # Utility scripts
├── streamlit/              # Dashboard application
├── tests/                  # Test files
├── main.py                 # Application entry point
├── logger.py               # Monitoring configuration
└── requirements.txt        # Python dependencies
```
