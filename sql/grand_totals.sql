with most_popular as (
  select sku
  from {{clean_table_id}} 
  group by 1
  order by count(*) desc
  limit 1
),

least_popular as (
  select sku
  from {{clean_table_id}} 
  group by 1
  order by count(*)
  limit 1
)

select
  json_object(
    'total_orders', count(distinct order_id),
    'gross_sales', sum(item_price + item_tax + shipping_price + shipping_tax +
      gift_wrap_price + gift_wrap_tax),
    'net_sales', sum(item_price - item_promo_discount),
    'grand_total', sum(item_price + item_tax + shipping_price + shipping_tax +
      gift_wrap_price + gift_wrap_tax
      - (item_promo_discount + shipment_promo_discount)),
    'most_popular_product_sku', max(most_popular.sku),
    'least_popular_product_sku', max(least_popular.sku)
  ) as grand_totals
from {{clean_table_id}} , most_popular, least_popular;