CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    product_id VARCHAR(20) NOT NULL,
    description VARCHAR(100),
    invoice_id VARCHAR(50) NOT NULL,
    sales_channel VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    is_returned BOOLEAN NOT NULL DEFAULT FALSE,
    total NUMERIC(10, 2) NOT NULL
);

/*CREATE VIEW IF NOT EXISTS dim_product AS (
    SELECT DISTINCT
        product_id,
        description
    FROM raw_transactions
)
CREATE VIEW IF NOT EXISTS dim_customer AS (
    SELECT DISTINCT
        customer_id,
        customer_name,
        country
    FROM raw_transactions
);
CREATE VIEW IF NOT EXISTS fact_sales AS (
    SELECT
        transaction_id,
        timestamp,
        customer_id,
        product_id,
        invoice_id,
        sales_channel,
        quantity,
        price,
        is_returned,
        total
    FROM raw_transactions
);
CREATE VIEW IF NOT EXISTS fact_invoice AS (
    SELECT
        invoice_id,
        SUM(total_amount) AS total_amount,
        COUNT(DISTINCT transaction_id) AS transaction_count
    FROM raw_transactions
    GROUP BY invoice_id
);*/