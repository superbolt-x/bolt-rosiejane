{{ config (
    alias = target.database + '_googleads_campaign_product_performance'
)}}

WITH googleads_parsed AS (
    SELECT 
        *,
        -- Extract product_id (3rd segment) from product_item_id
        SPLIT_PART(product_item_id, '_', 3) AS extracted_product_id,
        -- Extract variant_id (4th segment) from product_item_id
        SPLIT_PART(product_item_id, '_', 4) AS extracted_variant_id
    FROM {{ ref('googleads_performance_by_campaign_product') }}
)
    
SELECT 
    account_id,
    campaign_name,
    campaign_id,
    product_item_id,
    g.product_title as product_title,
    s.product_title as sho_product_title,
    s.product_type as sho_product_type,
    campaign_status,
    campaign_type_default,
    date,
    date_granularity,
    spend,
    impressions,
    clicks,
    conversions as purchases,
    conversions_value as revenue
FROM googleads_parsed g
LEFT JOIN {{ source('shopify_base','shopify_products') }} s
    ON s.product_id = g.extracted_product_id AND s.variant_id = g.extracted_variant_id
