-- create or replace table tokopedia-970.sandbox_merchant.ft_market_share_brand 
-- partition by verified_date as

with
-- Schedule tiap Minggu sore
date_dict as (
select -- Ambil data dari awal 2021 sampai Hari Sabtu minggu lalu
  date('2021-01-01') start_date, 
  current_date('+07') end_date ),  -- Tanggal sekarang

brand_annotation as 
(select pa.*, child_cat_id
from (select product_id, annotation_value_name brand,row_number() over (partition by product_id, annotation_type_name order by processed_dttm desc) as rankk
-- from tokopedia-970.zoom.dt_product_annotation
from tokopedia-970.sandbox_discovery.da_fact_product_annotation
where annotation_type_name = 'Merek') pa
left join tokopedia-970.zoom.dt_product using(product_id)
left join (select * from tokopedia-970.zoom.dt_product_category where current_ind = 1) using(child_cat_id)
where rankk = 1),

additional_brand_annotation as 
(select child_cat_id, product_id, concat("zzDS - ",bds.brand) AS brand
from brand_annotation bds
left join (select child_cat_id_by_order, brand 
          from `tokopedia-970.sandbox_merchant.dm_market_share_brand` group by 1,2) bda on child_cat_id = child_cat_id_by_order and  bda.brand = bds.brand 
where bda.brand is null
group by 1,2,3),

base_fact as (
select product_id, product_name, fod.shop_id, 
       fod.child_cat_id child_cat_id_by_order, fod.level1_cat_id, fod.level2_cat_id, fod.level3_cat_id,
       date(payment_time) verified_date,
       fod.order_id,
       order_dtl_id,
       com.subtotal_price/com.quantity as product_price
from tokopedia-970.zoom.ft_order_detail_marketplace fod
inner join tokopedia-970.sandbox_merchant.v_sales_performance_v2 com using(order_dtl_id, product_id)
where verified_month between (select start_date from date_dict) and (select end_date from date_dict)
group by 1,2,3,4,5,6,7,8,9,10,11 ),

fact_result as (
select
  verified_date,
  order_id,
  order_dtl_id,
  product_id,
  child_cat_id_by_order,
  priority,
  min_price,
  case when ds.brand is not null THEN TRUE else is_no_negative_keyword end is_no_negative_keyword,
  case when product_price >= ifnull(min_price,0) then true
  when ds.brand is not null THEN TRUE else false end as is_range_price,
  brand_domain,
  brand_keyword,
  case when da.brand is not null then da.brand when da.brand is null and ds.brand is not null then ds.brand else 'Unbranded' end brand,
  brand_grouping,
  case when st.shop_id is not null then 1 else 0 end as os_brand_curration_tagging 
from base_fact
left join `tokopedia-970.sandbox_merchant.dm_market_share_brand` da using (product_id, product_name, child_cat_id_by_order)
left join additional_brand_annotation ds using(product_id)
left join (select shop_id from `tokopedia-970.sandbox_merchant.da_market_share_shop_brand_tagging` where is_os = 'OS Brand Official' group by 1) st using(shop_id)
)

select * 
from fact_result

