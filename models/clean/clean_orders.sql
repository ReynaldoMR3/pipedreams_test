/*
Here we are going to filter by delivered order status and test that
To calculate the metrics we are only using delivered orders,
because are the orders that are already “finished” and that the client has already paid for and received. 
If the team wants to add another status to the calculations we can add them.
Also we are converting all the timestamps to date, to manage the daily granularity that the business requires.
*/
with source as (
    select * from {{source('olist_datasets', 'OLIST_ORDERS_DATASET')}}
),

valid_orders as (
    select
        CUSTOMER_ID,
        ORDER_ID,
        ORDER_STATUS,
        DATE_TRUNC('DAY', ORDER_PURCHASE_TIMESTAMP::timestamp)::date      as ORDER_PURCHASE_TIMESTAMP,
        DATE_TRUNC('DAY', ORDER_APPROVED_AT::timestamp)::date             as ORDER_APPROVED_AT,
        DATE_TRUNC('DAY', ORDER_ESTIMATED_DELIVERY_DATE::timestamp)::date as ORDER_ESTIMATED_DELIVERY_DATE,
        DATE_TRUNC('DAY', ORDER_DELIVERED_CARRIER_DATE::timestamp)::date  as ORDER_DELIVERED_CARRIER_DATE,
        DATE_TRUNC('DAY', ORDER_DELIVERED_CUSTOMER_DATE::timestamp)::date as ORDER_DELIVERED_CUSTOMER_DATE
    from source
    where ORDER_STATUS = 'delivered'
)

select * from valid_orders