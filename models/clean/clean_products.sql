/*
Total revenue by day is going to be calculated using (price * item quantity) - freight_value. 
*/

with source as (
    select * from {{source('olist_datasets', 'OLIST_ORDER_ITEMS_DATASET')}}
),

clean_products as (
    select
        ORDER_ID,
        PRODUCT_ID,
        PRICE,
        FREIGHT_VALUE
    from source
),

products_with_valid_orders as (
    select
        clean_orders.ORDER_PURCHASE_TIMESTAMP as ORDER_PURCHASE_TIMESTAMP,
        clean_orders.ORDER_ID                 as ORDER_ID,
        PRODUCT_ID                            as PRODUCT_ID ,
        sum(PRICE)                            as PRICE,
        sum(FREIGHT_VALUE)                    as FREIGHT_VALUE
    from {{ ref('clean_orders') }} as clean_orders
    join clean_products -- inner join to have only the delivered orders
    ON clean_products.ORDER_ID = clean_orders.ORDER_ID
    {{ dbt_utils.group_by(3) }} -- performing group by operation by the first 3 columns, 
                                -- we are grouping by in this step to have the aggs needed for the next model. 
                                -- (if an order id contains n times the same product we wan't to have that aggregated)
)

select * from products_with_valid_orders