with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
)


Select 
    o.order_id
    , c.customer_id
    , amount
from customers c
inner join orders o on c.customer_id=o.customer_id
inner join payments p on o.order_id=p.order_ID
