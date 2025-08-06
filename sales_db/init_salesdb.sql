CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    invoice_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    country VARCHAR(50),
    product_id VARCHAR(20) NOT NULL,
    description VARCHAR(100),
    sales_channel VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    is_returned BOOLEAN NOT NULL DEFAULT FALSE,
    total NUMERIC(10, 2) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_transactions_invoice_id ON raw_transactions (invoice_id);
CREATE INDEX IF NOT EXISTS idx_raw_transactions_timestamp ON raw_transactions (timestamp);