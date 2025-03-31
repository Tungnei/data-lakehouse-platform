{{ config(
    materialized='incremental',
    unique_key='sales_id'
) }}

WITH stg_sales AS (
    SELECT 
        _id AS sales_id,
        timestamp,
        store_id,
        payment_method,
        jsonb_array_elements(items) AS item
    FROM {{ source('raw', 'transactions') }}

    {% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
    {% endif %}
)

SELECT
    stg_sales.sales_id,
    stg_sales.timestamp,
    stg_sales.store_id::INTEGER,
    stg_sales.payment_method::INTEGER AS payment_method_id,
    (item->>'product_id') AS product_id,
    (item->>'quantity')::INTEGER AS quantity,
    (item->>'subtotal')::INTEGER AS subtotal
FROM stg_sales


