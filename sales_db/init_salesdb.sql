CREATE TABLE IF NOT EXISTS fact_product_sales (
    transaction_id SERIAL PRIMARY KEY,
    timestamp_id INTEGER NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    invoice_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    is_returned BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (timestamp_id) REFERENCES dim_timestamp(timestamp_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
    FOREIGN KEY (invoice_id) REFERENCES dim_invoice(invoice_id)
);
CREATE TABLE IF NOT EXISTS dim_timestamp (
    timestamp_id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    second INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    country VARCHAR(50),
);
CREATE TABLE IF NOT EXISTS dim_product (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
);
CREATE TABLE IF NOT EXISTS dim_invoice (
    invoice_id VARCHAR(50) PRIMARY KEY,
    sales_channel VARCHAR(50) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL);