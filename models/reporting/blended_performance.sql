{{ config (
    alias = target.database + '_blended_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
  
WITH paid_data as
    (SELECT channel, date_granularity, date::date, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(revenue),0) as revenue
    FROM
        (SELECT 'Meta Sephora' as channel, date, date_granularity, 
            spend, link_clicks as clicks, impressions, purchases, revenue
        FROM {{ source('reporting','facebook_campaign_performance') }}
        WHERE campaign_name ~* 'sephora'
        UNION ALL
        SELECT 'Meta DTC' as channel, date, date_granularity, 
            spend, link_clicks as clicks, impressions, purchases, revenue
        FROM {{ source('reporting','facebook_campaign_performance') }}
        WHERE campaign_name !~* 'sephora'
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity, 
            spend, clicks, impressions, purchases, revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT 'TikTok DTC' as channel, date, date_granularity, 
            spend, clicks, impressions, purchases, revenue
        FROM {{ source('reporting','tiktok_ad_performance') }}
        WHERE campaign_name !~* 'traffic'
        UNION ALL
        SELECT 'TikTok Sephora' as channel, date, date_granularity, 
            spend, clicks, impressions, purchases, revenue
        FROM {{ source('reporting','tiktok_ad_performance') }}
        WHERE campaign_name ~* 'traffic'
        UNION ALL
        SELECT 'Tiktok DTC' as channel, date_trunc('day',date) as date, 'day' as date_granularity, 
            sum(spend) as spend, sum(0) as clicks, sum(0) as impressions, sum(purchases) as purchases, sum(revenue) as revenue
        FROM {{ source('gsheet_raw','tiktok_gmv_campaigns_insights') }}
        GROUP BY 1,2,3
        UNION ALL
        SELECT 'Tiktok DTC' as channel, date_trunc('week',date) as date, 'week' as date_granularity, 
            sum(spend) as spend, sum(0) as clicks, sum(0) as impressions, sum(purchases) as purchases, sum(revenue) as revenue
        FROM {{ source('gsheet_raw','tiktok_gmv_campaigns_insights') }}
        GROUP BY 1,2,3
        UNION ALL
        SELECT 'Tiktok DTC' as channel, date_trunc('month',date) as date, 'month' as date_granularity, 
            sum(spend) as spend, sum(0) as clicks, sum(0) as impressions, sum(purchases) as purchases, sum(revenue) as revenue
        FROM {{ source('gsheet_raw','tiktok_gmv_campaigns_insights') }}
        GROUP BY 1,2,3
        UNION ALL
        SELECT 'Tiktok DTC' as channel, date_trunc('quarter',date) as date, 'quarter' as date_granularity, 
            sum(spend) as spend, sum(0) as clicks, sum(0) as impressions, sum(purchases) as purchases, sum(revenue) as revenue
        FROM {{ source('gsheet_raw','tiktok_gmv_campaigns_insights') }}
        GROUP BY 1,2,3
        UNION ALL
        SELECT 'Tiktok DTC' as channel, date_trunc('year',date) as date, 'year' as date_granularity, 
            sum(spend) as spend, sum(0) as clicks, sum(0) as impressions, sum(purchases) as purchases, sum(revenue) as revenue
        FROM {{ source('gsheet_raw','tiktok_gmv_campaigns_insights') }}
        GROUP BY 1,2,3
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
