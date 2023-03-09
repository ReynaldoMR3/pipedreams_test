with agg_orders as (
    select
        ORDER_PURCHASE_TIMESTAMP                 as order_purchase_date,
        COUNT(ORDER_ID)                          as orders_count,                   -- grouping by date to count the total orders
        CAST(COUNT(CUSTOMER_ID) as NUMBER(10,2)) as customers_making_orders_count   -- grouping by date to count the total customers making orders.
    from {{ ref('clean_orders') }}
    group by order_purchase_date
),

revenue_by_day as (
    select
        ORDER_PURCHASE_TIMESTAMP   as order_purchase_date,
        SUM(PRICE - FREIGHT_VALUE) as revenue_usd -- grouping by day to sum the difference of the price and the freight value to obtain the daily revenue.
    from {{ref('clean_products')}}
    group by ORDER_PURCHASE_TIMESTAMP
),

-- grouping by day/order_id to sum the difference of the price and the freight value
-- this is going to help us have the same day n times and be able to perform the avg on the following cte
revenue_by_order_day as (
    select
        ORDER_PURCHASE_TIMESTAMP   as order_purchase_date,
        SUM(PRICE - FREIGHT_VALUE) as revenue_per_day_and_order 
    from {{ref('clean_products')}}
    group by ORDER_ID, ORDER_PURCHASE_TIMESTAMP
),

-- calculating the average revenue per order using the previuos cte by day.
avg_revenue_by_order_day as (
    select
        order_purchase_date                                  as order_purchase_date,
        CAST(AVG(revenue_per_day_and_order) as NUMBER(10,2)) as average_revenue_per_order_usd
    from revenue_by_order_day
    group by order_purchase_date
),

-- sorting categories to calculate the row number to help us as a rank
-- also using a window function to calculate the daily revenue so we can get the percentages needed in the next cte

sorted_categories as (
    select
        order_purchase_date,
        category,
        category_revenue,
        row_number() over (partition by order_purchase_date order by category_revenue desc) as category_rank, -- the partition by day to match the other metrics.
        sum(category_revenue) over (partition by order_purchase_date) as daily_revenue -- getting the daily revenue per row so it's easier in the next cte to get percents.
    from {{ref('clean_categories')}}
),

categories_metrics as (
    select
        order_purchase_date,
        array_agg(category) as top_3_product_categories_by_revenue, -- getting the top 3 categories inside array.
        array_agg(round(category_revenue / daily_revenue * 100, 2)) as top_3_product_categories_revenue_percentage -- getting the percents inside arrays
    from sorted_categories
    WHERE category_rank <= 3 -- filtering only the top 3 categories
    GROUP BY order_purchase_date -- getting distinct values by day
),
-- using full outer joins to get all the rows from the tables and see if we have faulty data
orders_with_revenues as (
    select
        orders.order_purchase_date,
        orders_count,
        customers_making_orders_count,
        revenue_usd,
        average_revenue_per_order_usd,
        top_3_product_categories_by_revenue,
        top_3_product_categories_revenue_percentage
    from agg_orders as orders 
    full outer join revenue_by_day
        on orders.order_purchase_date = revenue_by_day.order_purchase_date
    full outer join avg_revenue_by_order_day
        on orders.order_purchase_date = avg_revenue_by_order_day.order_purchase_date
    full outer join categories_metrics
        on orders.order_purchase_date = categories_metrics.order_purchase_date
)

select * from orders_with_revenues