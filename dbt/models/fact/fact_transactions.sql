SELECT
    t.transaction_id,
    t.timestamp,
    t.invoice_id,
    t.customer_id,
    t.product_id,
    t.quantity,
    t.price,
    t.total,
    t.is_returned
FROM {{ ref('stg_transactions') }} as t