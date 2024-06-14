select 
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    {{ cents_to_dollars('amount', 'amount') }}, -- cents to dollers
    created as created_at
from {{ source('stripe', 'payment') }}