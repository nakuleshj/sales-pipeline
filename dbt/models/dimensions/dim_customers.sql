{{ config(materialized='view') }}

SELECT DISTINCT
    customer_id,
    country
FROM {{ ref('stg_transactions') }}
WHERE customer_id IS NOT NULL