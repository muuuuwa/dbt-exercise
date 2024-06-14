select 
    order_id, sum(amount) as total_amount
from {{ ref('stg_stripe__payments') }} 
where status = "success" 
group by order_id