WITH stg_sale AS (
    SELECT *
    FROM {{ ref('stg_sale') }}
)

SELECT
    MOD(ABS(CAST('x' || LEFT(MD5(CONCAT(stg_sale.sales_id, stg_sale.product_id)), 15) AS BIT(64))::BIGINT), 10000000000) AS sales_key,
    {{ dbt_utils.generate_surrogate_key(['stg_sale.store_id']) }} AS store_key,
    {{ dbt_utils.generate_surrogate_key(['stg_sale.product_id']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['stg_sale.payment_method_id']) }} AS payment_method_key,
    stg_sale.sales_id,
    stg_sale.timestamp,
    stg_sale.quantity,
    stg_sale.subtotal AS revenue
FROM stg_sale

