{{ config (
    alias = target.database + '_facebook_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue,
omni_purchase_with_shared_items as purchase_with_shared_items,
omni_purchase_with_shared_items_value as revenue_with_shared_items
FROM {{ ref('facebook_performance_by_campaign') }}
