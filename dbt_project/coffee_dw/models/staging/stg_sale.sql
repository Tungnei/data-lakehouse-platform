WITH stg_sale AS (
    SELECT 
        _id AS sales_id,
        timestamp,
        store_id,
        payment_method,
        jsonb_array_elements(items) AS item
    FROM {{ source('raw', 'transactions') }}
)

SELECT
    stg_sale.sales_id,
    stg_sale.timestamp,
    stg_sale.store_id::INTEGER,
    stg_sale.payment_method::INTEGER AS payment_method_id,
    (item->>'product_id') AS product_id,
    (item->>'quantity')::INTEGER AS quantity,
    (item->>'subtotal')::INTEGER AS subtotal
FROM stg_sale

