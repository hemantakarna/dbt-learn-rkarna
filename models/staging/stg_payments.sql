select 
"orderID" as order_ID
, Created
, sum(Amount) as amount

from raw.stripe.payment

group by 
"orderID"
, Created