{{ config(materialized='view') }}

SELECT DISTINCT
    invoice_id,
    sales_channel
FROM {{ ref('stg_transactions') }}