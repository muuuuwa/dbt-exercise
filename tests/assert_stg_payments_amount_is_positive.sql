with payments as (
    select 
    *
    from {{ ref('stg_stripe__payments') }}
)

select
    payment_id,
    sum(amount) as total_amount
from
    payments
group by
    payment_id
having
    total_amount < 0