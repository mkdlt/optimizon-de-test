with most_popular as (
  select
    date_trunc(purchased_date, month) as period,
    sku
  from {{clean_table_id}} 
  group by 1, 2
  qualify rank() over (partition by period order by count(*) desc) = 1
  order by count(*) desc
),

least_popular as (
  select
    date_trunc(purchased_date, month) as period,
    sku
  from {{clean_table_id}} 
  group by 1, 2
  qualify rank() over (partition by period order by count(*)) = 1
  order by count(*)
),

json_objects as (
  select
    date_trunc(purchased_date, month) as period,
    json_object(
      'period', max(date_trunc(purchased_date, month)),
      'total_orders', count(distinct order_id),
      'gross_sales', sum(item_price + item_tax + shipping_price + shipping_tax
        + gift_wrap_price + gift_wrap_tax),
      'net_sales', sum(item_price - item_promo_discount),
      'grand_total', sum(item_price + item_tax + shipping_price + shipping_tax
        + gift_wrap_price + gift_wrap_tax
        - (item_promo_discount + shipment_promo_discount)),
      'most_popular_product_sku', max(most_popular.sku),
      'least_popular_product_sku', max(least_popular.sku)
    ) as metrics
  from {{clean_table_id}}  as clean
  inner join most_popular
    on date_trunc(purchased_date, month) = most_popular.period
  inner join least_popular
    on date_trunc(purchased_date, month) = least_popular.period
  group by 1
  order by 1
)

select
  json_object(
    'group_by', 'month',
    'start_date', min(period),
    'end_date', max(period),
    'metrics', array_agg(metrics)
  ) as metrics_monthly
from json_objects;