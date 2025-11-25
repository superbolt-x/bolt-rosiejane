{{ config (
    alias = target.database + '_tiktok_campaign_performance'
)}}

WITH platform_data as (
  SELECT 
  campaign_name,
  campaign_id,
  campaign_status,
  campaign_type_default,
  date,
  date_granularity,
  cost as spend,
  impressions,
  clicks,
  complete_payment as purchases,
  total_complete_payment_rate as revenue,
  web_event_add_to_cart as atc
  FROM {{ ref('tiktok_performance_by_campaign') }}
)

, gmv_max_data as (
SELECT campaign_name, campaign_id, '(not set)' as campaign_status, 'GMV Max' as campaign_type_default, 
date_trunc('day',date) as date, 'day' as date_granularity, 
sum(spend) as spend, sum(0) as impressions, sum(0) as clicks, sum(purchases) as purchases, sum(revenue) as revenue, sum(0) as atc
FROM {{ source('gsheet_raw','tiktok_gmv_campaigns_insights') }}
GROUP BY 1,2,3,4,5,6        
UNION ALL
SELECT campaign_name, campaign_id, '(not set)' as campaign_status, 'GMV Max' as campaign_type_default, 
date_trunc('week',date) as date, 'week' as date_granularity, 
sum(spend) as spend, sum(0) as impressions, sum(0) as clicks, sum(purchases) as purchases, sum(revenue) as revenue, sum(0) as atc
FROM {{ source('gsheet_raw','tiktok_gmv_campaigns_insights') }}
GROUP BY 1,2,3,4,5,6        
UNION ALL
SELECT campaign_name, campaign_id, '(not set)' as campaign_status, 'GMV Max' as campaign_type_default, 
date_trunc('month',date) as date, 'month' as date_granularity, 
sum(spend) as spend, sum(0) as impressions, sum(0) as clicks, sum(purchases) as purchases, sum(revenue) as revenue, sum(0) as atc
FROM {{ source('gsheet_raw','tiktok_gmv_campaigns_insights') }}
GROUP BY 1,2,3,4,5,6        
UNION ALL
SELECT campaign_name, campaign_id, '(not set)' as campaign_status, 'GMV Max' as campaign_type_default, 
date_trunc('quarter',date) as date, 'quarter' as date_granularity, 
sum(spend) as spend, sum(0) as impressions, sum(0) as clicks, sum(purchases) as purchases, sum(revenue) as revenue, sum(0) as atc
FROM {{ source('gsheet_raw','tiktok_gmv_campaigns_insights') }}
GROUP BY 1,2,3,4,5,6        
UNION ALL
SELECT campaign_name, campaign_id, '(not set)' as campaign_status, 'GMV Max' as campaign_type_default, 
date_trunc('year',date) as date, 'year' as date_granularity, 
sum(spend) as spend, sum(0) as impressions, sum(0) as clicks, sum(purchases) as purchases, sum(revenue) as revenue, sum(0) as atc
FROM {{ source('gsheet_raw','tiktok_gmv_campaigns_insights') }}
GROUP BY 1,2,3,4,5,6
)

SELECT * FROM platform_data
UNION ALL
SELECT * FROM gmv_max_data
