{{
  config(
    materialized='view'
  )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select 
    o.order_id
    , o.customer_id
    , o.order_date
    , sum(co.amount) as amount
    from {{ ref('stg_orders') }} o
    left outer join {{ ref('orders') }} co on o.order_id=co.order_id

    group by
    o.order_id
    , o.customer_id
    , o.order_date
),

customer_orders as (
    select
        o.customer_id,
        min(o.order_date) as first_order_date,
        max(o.order_date) as most_recent_order_date,
        count(o.order_id) as number_of_orders, 
        sum(o.amount) as lifetime_value
    from orders o

    group by o.customer_id
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(lifetime_value, 0) as lifetime_value
    from customers
    left join customer_orders using (customer_id)
)
select * from final