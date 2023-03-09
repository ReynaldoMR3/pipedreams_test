with source as (
    select * from {{source('olist_datasets', 'OLIST_PRODUCTS_DATASET')}}
),

categories as (
    select
        PRODUCT_ID            as product_id,
        PRODUCT_CATEGORY_NAME as category
    from source
),

-- calculating revenue per product_id and day. 
product_daily_revenue as (
    select
        ORDER_PURCHASE_TIMESTAMP   as order_purchase_date,
        PRODUCT_ID                 as product_id,
        SUM(PRICE - FREIGHT_VALUE) as product_revenue
    from {{ref('clean_products')}}
    {{ dbt_utils.group_by(2) }} -- performing group by operation by the first 2 columns
),

-- joinin the information by product id and aggregating by category and date
-- this is going to help us in the metrics calculations requiring arrays by top 3 categories per day.
categories_with_revenues as (
    select
        order_purchase_date,
        category,
        sum(product_revenue) as category_revenue
    from categories
    join product_daily_revenue as revenue
        on categories.product_id = revenue.product_id
    {{ dbt_utils.group_by(2) }} -- performing group by operation by the first 2 columns
)

select * from categories_with_revenues