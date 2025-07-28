create or replace table {{clean_table_id}} 
partition by purchased_date as

with cleaned_values as (
    select
        regexp_extract(regexp_replace(order_id, '€', 'e'),
            r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}') as order_id,
        parse_timestamp('%Y-%m-%dT%H:%M:%E*S', regexp_extract(purchased_at,
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)')) as purchased_at,
        parse_date('%Y-%m-%d', regexp_extract(purchased_date,
            r'\d{4}-\d{2}-\d{2}')) AS purchased_date,
        parse_date('%Y-%m-%d', regexp_extract(purchased_month_ended,
            r'\d{4}-\d{2}-\d{2}')) AS purchased_month_ended,
        regexp_extract(order_item_id, r'\d{14}') as order_item_id,
        regexp_extract(sku, r'\w{3}-\d{8}') as sku,
        trim(regexp_replace(regexp_replace(product_title,
            r'(, )?[^a-zA-Z0-9]{2,3}$', ''), '€', 'e')) as product_title,
        trim(regexp_replace(regexp_replace(product_name_full,
            r'(, )?[^a-zA-Z0-9]{2,3}$', ''), '€', 'e')) as product_name_full,
        regexp_extract(currency, r'[A-Z]{3}') as currency,
        cast(regexp_extract(item_price,
            r'\d+\.\d{1,2}') as numeric) as item_price,
        cast(regexp_extract(item_tax,
            r'\d+\.\d{1,2}') as numeric) as item_tax,
        cast(regexp_extract(shipping_price,
            r'\d+\.\d{1,2}') as numeric) as shipping_price,
        cast(regexp_extract(shipping_tax,
            r'\d+\.\d{1,2}') as numeric) as shipping_tax,
        cast(regexp_extract(gift_wrap_price,
            r'\d+\.\d{1,2}') as numeric) as gift_wrap_price,
        cast(regexp_extract(gift_wrap_tax,
            r'\d+\.\d{1,2}') as numeric) as gift_wrap_tax,
        cast(regexp_extract(item_promo_discount,
            r'\d+\.\d{1,2}') as numeric) as item_promo_discount,
        cast(regexp_extract(shipment_promo_discount,
            r'\d+\.\d{1,2}') as numeric) as shipment_promo_discount,
        regexp_replace(regexp_replace(ship_service_level,
            r'[^a-zA-Z0-9]+$', ''), '€', 'e') as ship_service_level
    from {{raw_table_id}}
),

deduplicated as (
    select
        *,
        row_number() over (partition by order_item_id) as row_num
    from cleaned_values
    qualify row_num = 1
)

select * except (row_num)
from deduplicated;
