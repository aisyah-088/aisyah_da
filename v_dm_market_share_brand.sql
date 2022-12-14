-- DA PGSO

-- create or replace table tokopedia-970.sandbox_merchant.dm_market_share_brand as
-- partition by verified_date as

with
-- Schedule tiap Minggu sore
date_dict as (
select -- Ambil data dari awal 2021 sampai sekarang
  date('2021-01-01') start_date, 
  current_date('+07') end_date ),  -- Tanggal sekarang

base_dim as (
select product_id, product_name, fod.shop_id, 
       fod.child_cat_id child_cat_id_by_order, fod.level1_cat_id, fod.level2_cat_id, fod.level3_cat_id,
from tokopedia-970.zoom.ft_order_detail_marketplace fod
inner join tokopedia-970.sandbox_merchant.v_sales_performance_v2 using(order_dtl_id, product_id)
where verified_month between (select start_date from date_dict) and (select end_date from date_dict)
group by 1,2,3,4,5,6,7 ),

-- List of brands and their keywords (from sheet)
list_brand_input as (
select * except(keyword_series),
       lower(keyword_series) keyword_series,
       case  when level3_cat is not null then 3
             when level2_cat is not null then 2
             when level1_cat is not null then 1
       end level_detail,
       1 join_key
from `tokopedia-970.sandbox_merchant.da_market_share_brand_keyword` ),
 
list_negative_keyword_input as (
select distinct * except(negative_keyword),
       lower(negative_keyword) negative_keyword_series,
       case when level_2 is not null then 2
           when level_1 is not null then 1
       end level_detail,
       1 join_key
from `tokopedia-970.sandbox_merchant.da_market_share_brand_negative_keyword`
where type = 'exclude word' )

, product_keyword_pair as (
select
  product_id,
  product_name_origin product_name,
  child_cat_id_by_order,
  pc.level1_cat_id level1_id,
  pc.level2_cat_id level2_id,
  pc.level3_cat_id level3_id,
  duplicate_flag,
  ifnull(priority,1) priority,
  min_price,
  input_date,
  brand_tagging as brand_domain,
  ifnull(brand_tagging,brand) brand,
  brand_grouping brand_grouping,
  brand brand_keyword,
  case when ki.negative_keyword_series is null then true else false end as is_no_negative_keyword,
  -- case when comsubtotal_price/com.quantity >= ifnull(com.subtotal_price/com.quantity,0) then true else false end as is_range_price
from (
  select 
    * except(product_name),
    product_name product_name_origin, 
    case when lower(product_name) like 'beli _ gratis _ %' then product_name 
         else REGEXP_REPLACE(product_name, 
              r"\bbukan.*|\bfree.*|\bbkn.*|\bseperti.*|\bspt.*|\blike.*|\bmirip.*|\bgratis.*|\bbonus", "") 
    end product_name 
  from base_dim ) pc
inner join list_brand_input bi on 1 = bi.join_key
      and ((bi.level_detail=3 and pc.level3_cat_id=bi.level3_id and pc.level2_cat_id=bi.level2_id and pc.level1_cat_id=bi.level1_id) 
        or (bi.level_detail=2 and pc.level2_cat_id=bi.level2_id  and pc.level1_cat_id=bi.level1_id) 
        or (bi.level_detail=1 and pc.level1_cat_id=bi.level1_id))
      and case when is_regex_keyword then regexp_contains(lower(pc.product_name), lower(keyword_series))
               else (regexp_contains(lower(pc.product_name), concat(' ',regexp_replace(lower(keyword_series),' ','.'),' '))
                  or regexp_contains(lower(product_name), concat(' ',regexp_replace(lower(keyword_series),' ','.'), '$'))
                  or regexp_contains(lower(product_name), concat('^', regexp_replace(lower(keyword_series),' ','.'),' ')) 
                  or regexp_contains(lower(product_name), concat('^', regexp_replace(lower(keyword_series),' ','.'),'$'))) 
          end  
left join list_negative_keyword_input ki 
     on ((ki.level_detail=2 and  pc.level2_cat_id=ki.level2_id  and  pc.level1_cat_id=ki.level1_id) 
      or (ki.level_detail=1 and  pc.level1_cat_id=ki.level1_id))
     and (regexp_contains(lower(product_name), concat(' ',lower(negative_keyword_series),' '))
       or regexp_contains(lower(product_name), concat(' ',lower(negative_keyword_series), '$'))
       or regexp_contains(lower(product_name), concat('^', lower(negative_keyword_series),' '))
       or regexp_contains(lower(product_name), concat('^', lower(negative_keyword_series),'$')))
left join `tokopedia-970.sandbox_merchant.da_market_share_shop_brand_tagging` b using(shop_id)
group by 1,2,3,4,5,5,6,7,8,9,10,11,12,13,14,15 )
 
, multiple_brand_prio as
(select child_cat_id_by_order,product_id, product_name, min(ifnull(priority,1) ) as priority, count(distinct brand) as uniq_brands
from product_keyword_pair
group by 1,2,3 )
 
-- For multiple brand tagging:
-- 1. Only select the first brand priority, if priority number is available (not null) --> mostly electronic handphone category
-- 2. Insert into array
, summary_brand_tagging as (
select
  product_id,
  product_name,
  child_cat_id_by_order,
  level1_id,
  level2_id,
  level3_id,
  b.priority,
  min_price,
  is_no_negative_keyword,
  ARRAY_AGG(brand_domain) brand_domain,
  ARRAY_AGG(brand_keyword) brand_keyword,
  ARRAY_AGG(brand) brand,
  ARRAY_AGG(brand_grouping) brand_grouping,
from  product_keyword_pair  b
inner join multiple_brand_prio p1 using(child_cat_id_by_order, product_id,product_name, priority)
group by 1,2,3,4,5,6,7,8,9 )
 
SELECT * except (brand, brand_grouping,brand_keyword,brand_domain),
  ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST(brand_domain) AS x ORDER BY x),", ") AS brand_domain,
  ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST(brand_keyword) AS x ORDER BY x),", ") AS brand_keyword,
  ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST(brand) AS x ORDER BY x),", ") AS brand,
  ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST(brand_grouping) AS x ORDER BY x),", ") AS brand_grouping,
  datetime(current_timestamp) as processed_dttm
FROM summary_brand_tagging
where array_length(brand) > 1

union all 

SELECT *  except (brand, brand_grouping,brand_keyword,brand_domain),
  ARRAY_TO_STRING(brand_domain,", ") brand_domain,
  ARRAY_TO_STRING(brand_keyword,", ") brand_keyword,
  ARRAY_TO_STRING(brand,", ") brand,
  ARRAY_TO_STRING(brand_grouping,", ") brand_grouping,
  datetime(current_timestamp) as processed_dttm
FROM summary_brand_tagging
where array_length(brand) = 1