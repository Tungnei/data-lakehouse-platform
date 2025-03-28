WITH stg_store AS (
    SELECT *
    FROM {{ ref('store') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['stg_store.id']) }} AS store_key,
    stg_store.id AS store_id,
    stg_store.name AS store_name,
    stg_store.address,
    stg_store.district
FROM stg_store