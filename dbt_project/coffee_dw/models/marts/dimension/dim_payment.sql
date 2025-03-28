WITH stg_payment_method AS(
    SELECT * 
    FROM {{ ref('payment_method') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_payment_method.id']) }} AS payment_method_key,
    stg_payment_method.id AS payment_method_id,
    stg_payment_method.method AS payment_method,
    stg_payment_method.modified_date AT TIME ZONE 'Asia/Ho_Chi_Minh'
FROM stg_payment_method