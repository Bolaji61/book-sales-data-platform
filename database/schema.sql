-- Book Sales Data Warehouse Schema
-- Star Schema Design with Fact and Dimension Tables

-- DIMENSION TABLES

-- Users Dimension Table
CREATE TABLE dim_users (
    user_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    location VARCHAR(255),
    signup_date DATE NOT NULL,
    social_security_number VARCHAR(20),
    state VARCHAR(50),
    city VARCHAR(100),
    user_segment VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Books Dimension Table
CREATE TABLE dim_books (
    book_id INTEGER PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    category VARCHAR(100) NOT NULL,
    base_price DECIMAL(10,2) NOT NULL,
    author VARCHAR(255) NOT NULL,
    isbn VARCHAR(20) NOT NULL,
    publication_year INTEGER NOT NULL,
    pages INTEGER,
    publisher VARCHAR(255),
    price_tier VARCHAR(20),
    age_category VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date Dimension Table
CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- FACT TABLES

-- Sales Fact Table
CREATE TABLE fact_sales (
    transaction_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    book_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    quantity INTEGER DEFAULT 1,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    transaction_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily Sales Summary
CREATE TABLE fact_daily_sales_summary (
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    total_revenue DECIMAL(15,2) NOT NULL,
    transaction_count INTEGER NOT NULL,
    unique_users INTEGER NOT NULL,
    average_transaction_value DECIMAL(10,2) NOT NULL,
    total_quantity INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_id)
);

-- Book Performance Summary
CREATE TABLE fact_book_performance (
    book_id INTEGER NOT NULL REFERENCES dim_books(book_id),
    total_sales INTEGER NOT NULL,
    total_revenue DECIMAL(15,2) NOT NULL,
    average_price DECIMAL(10,2) NOT NULL,
    unique_customers INTEGER NOT NULL,
    first_sale_date DATE,
    last_sale_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (book_id)
);

-- INDEXES FOR PERFORMANCE OPTIMIZATION

-- fact table indexes
CREATE INDEX idx_fact_sales_user_id ON fact_sales(user_id);
CREATE INDEX idx_fact_sales_book_id ON fact_sales(book_id);
CREATE INDEX idx_fact_sales_date_id ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_timestamp ON fact_sales(transaction_timestamp);
CREATE INDEX idx_fact_sales_amount ON fact_sales(amount);

-- composite indexes for common query patterns
CREATE INDEX idx_fact_sales_user_date ON fact_sales(user_id, date_id);
CREATE INDEX idx_fact_sales_book_date ON fact_sales(book_id, date_id);
CREATE INDEX idx_fact_sales_category_date ON fact_sales(book_id, date_id);

-- dimension table indexes
CREATE INDEX idx_dim_users_location ON dim_users(location);
CREATE INDEX idx_dim_users_signup_date ON dim_users(signup_date);
CREATE INDEX idx_dim_users_segment ON dim_users(user_segment);
CREATE INDEX idx_dim_users_state ON dim_users(state);

CREATE INDEX idx_dim_books_category ON dim_books(category);
CREATE INDEX idx_dim_books_author ON dim_books(author);
CREATE INDEX idx_dim_books_price_tier ON dim_books(price_tier);
CREATE INDEX idx_dim_books_publication_year ON dim_books(publication_year);

CREATE INDEX idx_dim_date_year ON dim_date(year);
CREATE INDEX idx_dim_date_month ON dim_date(month);
CREATE INDEX idx_dim_date_quarter ON dim_date(quarter);


-- Partition fact_sales by month for better query performance

-- Sales with all dimensions
CREATE VIEW v_sales_with_dimensions AS
SELECT 
    f.transaction_id,
    f.amount,
    f.quantity,
    f.transaction_timestamp,
    u.name as user_name,
    u.email as user_email,
    u.location as user_location,
    u.user_segment,
    b.title as book_title,
    b.category as book_category,
    b.author as book_author,
    b.base_price,
    d.full_date,
    d.year,
    d.month,
    d.quarter,
    d.day_name
FROM fact_sales f
JOIN dim_users u ON f.user_id = u.user_id
JOIN dim_books b ON f.book_id = b.book_id
JOIN dim_date d ON f.date_id = d.date_id;

-- Top performing books
CREATE VIEW v_top_books AS
SELECT 
    b.book_id,
    b.title,
    b.category,
    b.author,
    bp.total_sales,
    bp.total_revenue,
    bp.average_price,
    bp.unique_customers,
    RANK() OVER (ORDER BY bp.total_revenue DESC) as revenue_rank,
    RANK() OVER (ORDER BY bp.total_sales DESC) as sales_rank
FROM dim_books b
JOIN fact_book_performance bp ON b.book_id = bp.book_id;

-- User behavior analysis
CREATE VIEW v_user_behavior AS
SELECT 
    u.user_id,
    u.name,
    u.user_segment,
    u.signup_date,
    COUNT(DISTINCT f.transaction_id) as total_transactions,
    SUM(f.amount) as total_spent,
    AVG(f.amount) as avg_transaction_value,
    MIN(f.transaction_timestamp) as first_purchase,
    MAX(f.transaction_timestamp) as last_purchase,
    COUNT(DISTINCT f.book_id) as unique_books_purchased,
    COUNT(DISTINCT b.category) as unique_categories
FROM dim_users u
LEFT JOIN fact_sales f ON u.user_id = f.user_id
LEFT JOIN dim_books b ON f.book_id = b.book_id
GROUP BY u.user_id, u.name, u.user_segment, u.signup_date;

-- update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_dim_users_updated_at BEFORE UPDATE ON dim_users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dim_books_updated_at BEFORE UPDATE ON dim_books
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fact_sales_updated_at BEFORE UPDATE ON fact_sales
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ensure valid dates
ALTER TABLE dim_date ADD CONSTRAINT chk_valid_date CHECK (full_date >= '2020-01-01');

-- ensure valid publication years
ALTER TABLE dim_books ADD CONSTRAINT chk_valid_publication_year 
    CHECK (publication_year >= 1900 AND publication_year <= EXTRACT(YEAR FROM CURRENT_DATE) + 1);

-- ensure valid email format
ALTER TABLE dim_users ADD CONSTRAINT chk_valid_email 
    CHECK (email IS NULL OR email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
