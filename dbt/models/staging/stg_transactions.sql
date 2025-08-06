{{ config(materialized='view') }}

SELECT
    transaction_id,
    timestamp,
    invoice_id,
    customer_id,
    country,
    product_id,
    description,
    sales_channel,
    quantity,
    price,
    is_returned,
    total
FROM {{ source('raw', 'raw_transactions') }}