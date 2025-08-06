{{ config(materialized='view') }}

SELECT DISTINCT
    product_id,
    description
FROM {{ ref('stg_transactions') }}
WHERE product_id IS NOT NULL