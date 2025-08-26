{{ config (
    alias = target.database + '_blended_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
  
WITH paid_data as
    (SELECT channel, date_granularity, date::date, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(revenue),0) as revenue
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, 
            spend, link_clicks as clicks, impressions, purchases, revenue
        FROM {{ source('reporting','facebook_campaign_performance') }}
        WHERE campaign_name !~* 'traffic'
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity, 
            spend, clicks, impressions, purchases, revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity, 
            spend, clicks, impressions, purchases, revenue
        FROM {{ source('reporting','tiktok_ad_performance') }}
        WHERE campaign_name !~* 'traffic'
        )
    GROUP BY 1,2,3)
  
SELECT channel,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
    purchases,
    revenue
FROM paid_data
