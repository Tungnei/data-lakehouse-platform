WITH stg_product AS (
    SELECT * 
    FROM {{ ref('product') }}
),
     stg_product_category AS (
     SELECT *
     FROM {{ ref('product_category')}}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['stg_product.product_id']) }} AS product_key,
    stg_product.product_id,
    stg_product.name AS product_name,
    stg_product_category.name AS category_name,
    stg_product.price
FROM stg_product
LEFT JOIN stg_product_category
    ON stg_product.category_id = stg_product_category.id
