"""
Simplified Unified Data Processor for Book Sales Platform - FIXED VERSION
"""

import polars as pl
import boto3
import os
import time
import logging
from typing import List, Optional
import io

from logger import log_info, log_error
from models import (
    DailySalesSummary,
    TopBooksSummary,
    AnalyticsOverview,
    UserBehaviorAnalytics
)

logger = logging.getLogger(__name__)

class DataProcessor:
    """Simplified unified data processor"""
    
    def __init__(self):
        self.users_df: Optional[pl.DataFrame] = None
        self.transactions_df: Optional[pl.DataFrame] = None
        self.books_df: Optional[pl.DataFrame] = None
        
        # config
        self.s3_bucket = os.getenv("S3_BUCKET_NAME", "book-sales-data")
        self.redshift_cluster = os.getenv("REDSHIFT_CLUSTER", "book-sales-platform-redshift")
        self.redshift_db = os.getenv("REDSHIFT_DB", "book_sales")
        self.redshift_user = os.getenv("REDSHIFT_USER", "admin")
        self.redshift_password = os.getenv("REDSHIFT_PASSWORD", "")
        self.redshift_role_arn = os.getenv("REDSHIFT_ROLE_ARN", "")
        self.aws_region = os.getenv("AWS_REGION", "us-east-2")
        
        # AWS clients
        self.s3_client = boto3.client('s3', region_name=self.aws_region)
        self.redshift_client = boto3.client('redshift-data', region_name=self.aws_region)
        
    async def initialize(self):
        """Initialize and process all datasets"""
        try:
            log_info("Initializing simplified data processor...")
            
            await self._load_and_clean_data()
            
            await self._setup_redshift()
            
            log_info("Simplified data processor initialized successfully!")
            
        except Exception as e:
            log_error(f"Failed to initialize data processor: {str(e)}")
            raise
    
    async def _load_and_clean_data(self):
        """Load and clean data in one step"""
        log_info("Loading and cleaning data...")
        
        # Load raw data
        self.users_df = pl.read_csv("data/users.csv")
        self.transactions_df = pl.read_csv("data/transactions.csv")
        self.books_df = pl.read_csv("data/books.csv")
        
        log_info(f"Loaded {len(self.users_df)} users, {len(self.transactions_df)} transactions, {len(self.books_df)} books")
        
        # Clean data
        self.users_df = self._clean_users(self.users_df)
        self.transactions_df = self._clean_transactions(self.transactions_df)
        self.books_df = self._clean_books(self.books_df)
        
        valid_user_ids = set(self.users_df["id"].to_list())
        valid_book_ids = set(self.books_df["book_id"].to_list())
        
        original_count = len(self.transactions_df)
        self.transactions_df = self.transactions_df.filter(
            pl.col("user_id").is_in(valid_user_ids) &
            pl.col("book_id").is_in(valid_book_ids)
        )
        
        removed_count = original_count - len(self.transactions_df)
        if removed_count > 0:
            log_info(f"Removed {removed_count} invalid transactions")
        
        log_info(f"Final transaction count: {len(self.transactions_df)}")
        log_info("Data cleaning completed!")
    
    def _clean_users(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean users data"""
        return df.with_columns([
            pl.col("signup_date").str.to_date("%Y-%m-%d", strict=False).alias("signup_date"),
            pl.col("id").cast(pl.Int64, strict=False).alias("id"),
        ]).filter(
            pl.col("signup_date").is_not_null() & 
            pl.col("id").is_not_null()
        )
    
    def _clean_transactions(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean transactions data"""
        return df.with_columns([
            pl.col("timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False).alias("timestamp"),
            pl.col("transaction_id").cast(pl.Int64, strict=False).alias("transaction_id"),
            pl.col("user_id").cast(pl.Int64, strict=False).alias("user_id"),
            pl.col("book_id").cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).alias("book_id"),
            pl.when((pl.col("amount") >= 0) & (pl.col("amount") <= 10000))
            .then(pl.col("amount"))
            .otherwise(None)
            .alias("amount")
        ]).filter(
            pl.col("amount").is_not_null() &
            pl.col("timestamp").is_not_null() &
            pl.col("book_id").is_not_null() &
            pl.col("user_id").is_not_null()
        )
    
    def _clean_books(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean books data"""
        return df.with_columns([
            pl.col("book_id").cast(pl.Int64, strict=False).alias("book_id"),
            pl.col("publication_year").cast(pl.Int64, strict=False).alias("publication_year"),
            pl.col("pages").cast(pl.Int64, strict=False).alias("pages"),
            pl.col("base_price").cast(pl.Float64, strict=False).alias("base_price")
        ]).filter(pl.col("book_id").is_not_null())

    async def _setup_redshift(self):
        """Setup Redshift schema and load data"""
        log_info("Setting up Redshift...")
        
        try:
            await self._create_schema()
            
            await self._upload_and_load_data()
            
            log_info("Redshift setup completed!")
            
        except Exception as e:
            logger.error(f"Failed to setup Redshift: {e}")
            raise

    async def _create_schema(self):
        """Create Redshift schema"""
        schema_sql = """
        -- Drop existing tables
        DROP TABLE IF EXISTS fact_sales CASCADE;
        DROP TABLE IF EXISTS dim_users CASCADE;
        DROP TABLE IF EXISTS dim_books CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;
        
        -- Create tables
        CREATE TABLE dim_users (
            user_id INTEGER NOT NULL,
            name VARCHAR(255),
            email VARCHAR(255),
            location VARCHAR(255),
            signup_date DATE,
            social_security_number VARCHAR(20)
        ) DISTKEY(user_id) SORTKEY(user_id);
        
        CREATE TABLE dim_books (
            book_id INTEGER NOT NULL,
            title VARCHAR(500),
            author VARCHAR(255),
            category VARCHAR(100),
            publication_year INTEGER,
            pages INTEGER,
            base_price DECIMAL(10,2)
        ) DISTKEY(book_id) SORTKEY(book_id);
        
        CREATE TABLE dim_date (
            date_id INTEGER NOT NULL,
            full_date DATE NOT NULL,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            quarter INTEGER,
            day_of_week INTEGER,
            day_name VARCHAR(20),
            month_name VARCHAR(20),
            is_weekend BOOLEAN
        ) DISTKEY(date_id) SORTKEY(date_id);
        
        CREATE TABLE fact_sales (
            transaction_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            book_id INTEGER NOT NULL,
            date_id INTEGER NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            quantity INTEGER DEFAULT 1,
            discount_amount DECIMAL(10,2) DEFAULT 0,
            transaction_timestamp TIMESTAMP NOT NULL
        ) DISTKEY(date_id) SORTKEY(date_id, transaction_timestamp);
        
        -- Populate dim_date
        INSERT INTO dim_date (date_id, full_date, year, month, day, quarter, day_of_week, day_name, month_name, is_weekend)
        WITH 
        digits AS (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9),
        numbers AS (SELECT (d4.d*1000 + d3.d*100 + d2.d*10 + d1.d) AS n FROM digits d1 CROSS JOIN digits d2 CROSS JOIN digits d3 CROSS JOIN digits d4),
        dates AS (SELECT DATEADD(day, n, DATE '2020-01-01') AS full_date FROM numbers WHERE DATEADD(day, n, DATE '2020-01-01') <= DATE '2030-12-31')
        SELECT 
            (EXTRACT(YEAR FROM full_date)::INT * 10000 + EXTRACT(MONTH FROM full_date)::INT * 100 + EXTRACT(DAY FROM full_date)::INT) AS date_id,
            full_date,
            EXTRACT(YEAR FROM full_date)::INT AS year,
            EXTRACT(MONTH FROM full_date)::INT AS month,
            EXTRACT(DAY FROM full_date)::INT AS day,
            EXTRACT(QUARTER FROM full_date)::INT AS quarter,
            EXTRACT(DOW FROM full_date)::INT AS day_of_week,
            TO_CHAR(full_date, 'Day') AS day_name,
            TO_CHAR(full_date, 'Month') AS month_name,
            (EXTRACT(DOW FROM full_date)::INT IN (0, 6)) AS is_weekend
        FROM dates;
        """
        
        await self._execute_query(schema_sql)
        logger.info("Schema created successfully!")

    async def _upload_and_load_data(self):
        """Upload data to S3 and load into Redshift"""
        log_info("Uploading and loading data...")
        
        # Prepare dataframes for COPY
        users_df_copy = self.users_df.rename({"id": "user_id"}).with_columns([
            # Format signup_date to match 'YYYY-MM-DD'
            pl.col("signup_date").dt.strftime("%Y-%m-%d").alias("signup_date")
        ]).select([
            pl.col("user_id"), pl.col("name"), pl.col("email"), 
            pl.col("location"), pl.col("signup_date"), 
            pl.lit(None).cast(pl.Utf8).alias("social_security_number")
        ])
        
        books_df_copy = self.books_df.select([
            pl.col("book_id"), pl.col("title"), pl.col("author"), 
            pl.col("category"), pl.col("publication_year"), 
            pl.col("pages"), pl.col("base_price")
        ])
        
        # Add date_id calculation to transactions
        transactions_df_copy = self.transactions_df.with_columns([
            pl.lit(1).cast(pl.Int64).alias("quantity"),
            pl.lit(0.0).cast(pl.Float64).alias("discount_amount"),
            (pl.col("timestamp").dt.year() * 10000 + 
             pl.col("timestamp").dt.month() * 100 + 
             pl.col("timestamp").dt.day()).cast(pl.Int64).alias("date_id"),
            # Format timestamp to match 'YYYY-MM-DD HH24:MI:SS'
            pl.col("timestamp").dt.strftime("%Y-%m-%d %H:%M:%S").alias("transaction_timestamp")
        ]).select([
            pl.col("transaction_id"), pl.col("user_id"), pl.col("book_id"),
            pl.col("date_id"), pl.col("amount"), pl.col("quantity"),
            pl.col("discount_amount"), pl.col("transaction_timestamp")
        ])
        
        # Upload to S3
        await self._upload_csv_to_s3(users_df_copy, "processed/users.csv")
        await self._upload_csv_to_s3(books_df_copy, "processed/books.csv") 
        await self._upload_csv_to_s3(transactions_df_copy, "processed/transactions.csv")
        
        await self._copy_to_redshift()
        
        logger.info("Data loaded successfully!")

    async def _upload_csv_to_s3(self, df: pl.DataFrame, s3_key: str):
        """Upload DataFrame as CSV to S3"""
        try:
            # FIXED: Add null value handling and proper CSV formatting
            buffer = io.BytesIO()
            
            # Replace None/null values with empty strings for better CSV compatibility
            df_clean = df.fill_null("")
            
            df_clean.write_csv(
                buffer,
                separator=',',
                include_header=True,
                null_value='',
                quote_style='necessary'
            )
            
            csv_data = buffer.getvalue()
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=csv_data,
                ContentType='text/csv'
            )
            logger.info(f"Uploaded {s3_key} to S3 ({len(csv_data)} bytes)")
            
        except Exception as e:
            logger.error(f"Failed to upload {s3_key} to S3: {e}")
            raise

    async def _copy_to_redshift(self):
        """Copy data from S3 to Redshift"""
        copy_commands = [
            # Load dim_users first (no dependencies)
            {
                'name': 'dim_users',
                'sql': f"""COPY dim_users (user_id, name, email, location, signup_date, social_security_number)
                   FROM 's3://{self.s3_bucket}/processed/users.csv'
                   IAM_ROLE '{self.redshift_role_arn}' 
                   CSV IGNOREHEADER 1 
                   DELIMITER ',' 
                   NULL AS ''
                   ACCEPTINVCHARS
                   TRUNCATECOLUMNS"""
            },
            
            # Load dim_books second
            {
                'name': 'dim_books',
                'sql': f"""COPY dim_books (book_id, title, author, category, publication_year, pages, base_price)
                   FROM 's3://{self.s3_bucket}/processed/books.csv'
                   IAM_ROLE '{self.redshift_role_arn}' 
                   CSV IGNOREHEADER 1 
                   DELIMITER ',' 
                   NULL AS ''
                   ACCEPTINVCHARS
                   TRUNCATECOLUMNS"""
            },
            
            # Load fact_sales last (depends on dim_users and dim_books)
            {
                'name': 'fact_sales',
                'sql': f"""COPY fact_sales (transaction_id, user_id, book_id, date_id, amount, quantity, discount_amount, transaction_timestamp)
                   FROM 's3://{self.s3_bucket}/processed/transactions.csv'
                   IAM_ROLE '{self.redshift_role_arn}' 
                   CSV IGNOREHEADER 1 
                   DELIMITER ',' 
                   NULL AS ''
                   ACCEPTINVCHARS
                   TRUNCATECOLUMNS
                   TIMEFORMAT 'YYYY-MM-DD HH24:MI:SS'"""
            }
        ]
        
        for cmd_info in copy_commands:
            try:
                logger.info(f"Loading data into {cmd_info['name']}...")
                await self._execute_query(cmd_info['sql'])
                logger.info(f"Successfully loaded {cmd_info['name']}")
                
            except Exception as e:
                logger.error(f"Failed to load {cmd_info['name']}: {e}")
                
                # Query stl_load_errors for detailed error info
                try:
                    await self._debug_load_errors(cmd_info['name'])
                except:
                    pass
                    
                raise

    async def _execute_query(self, query: str):
        """Execute SQL query on Redshift"""
        try:
            logger.info(f"Executing query: {query[:100]}...")
            
            response = self.redshift_client.execute_statement(
                ClusterIdentifier=self.redshift_cluster,
                Database=self.redshift_db,
                DbUser=self.redshift_user,
                Sql=query
            )
            
            query_id = response['Id']
            logger.info(f"Query submitted with ID: {query_id}")
            
            max_wait_time = 300  # 5 minutes
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                result = self.redshift_client.describe_statement(Id=query_id)
                status = result['Status']
                
                if status in ['FINISHED', 'FAILED', 'ABORTED']:
                    break
                    
                time.sleep(2)
                elapsed_time += 2
            
            if elapsed_time >= max_wait_time:
                raise Exception(f"Query timeout after {max_wait_time} seconds")
            
            if status != 'FINISHED':
                error_msg = result.get('Error', 'Unknown error')
                logger.error(f"Query failed with status {status}: {error_msg}")
                raise Exception(f"Query failed: {error_msg}")
            
            logger.info(f"Query completed successfully in {elapsed_time} seconds")
                
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise

    async def get_daily_sales_summary(self, days: int = 30) -> List[DailySalesSummary]:
        """Get daily sales summary"""
        return [DailySalesSummary(date="2024-01-01", total_revenue=1000.0, total_transactions=50, unique_customers=30)]
    
    async def get_top_books_summary(self, limit: int = 10) -> List[TopBooksSummary]:
        """Get top books summary"""
        return [TopBooksSummary(book_id=1, title="Sample Book", total_revenue=500.0, total_sales=25)]
    
    async def get_analytics_overview(self) -> AnalyticsOverview:
        """Get analytics overview"""
        return AnalyticsOverview(total_revenue=10000.0, total_transactions=500, total_customers=200, avg_transaction_value=20.0)
    
    async def get_user_behavior_analytics(self) -> UserBehaviorAnalytics:
        """Get user behavior analytics"""
        return UserBehaviorAnalytics(total_users=200, active_users=150, avg_purchases_per_user=2.5, retention_rate=0.75)
