{{ config(
    materialized='incremental',
    unique_key='sales_key'
) }}

WITH stg_sales AS (
    SELECT *
    FROM {{ ref('stg_sales') }}

    {% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
    {% endif %}
)

SELECT
    MOD(ABS(CAST('x' || LEFT(MD5(CONCAT(stg_sales.sales_id, stg_sales.product_id)), 15) AS BIT(64))::BIGINT), 10000000000) AS sales_key,
    {{ dbt_utils.generate_surrogate_key(['stg_sales.store_id']) }} AS store_key,
    {{ dbt_utils.generate_surrogate_key(['stg_sales.product_id']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['stg_sales.payment_method_id']) }} AS payment_method_key,
    stg_sales.sales_id,
    stg_sales.timestamp,
    stg_sales.quantity,
    stg_sales.subtotal AS revenue
FROM stg_sales
