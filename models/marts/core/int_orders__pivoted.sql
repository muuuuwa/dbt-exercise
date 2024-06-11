{%- set payment_methods = ['credig_card', 'coupon', 'gift_card', 'bank_transfer'] %}

with payments as (
    select * 
    from {{ ref('stg_stripe__payments') }}
),
final as (
    select
        order_id,
        {%- for payment_method in payment_methods -%}
        sum( if(payment_method = "{{payment_method}}", amount, 0) ) as {{payment_method}}_card_amount
        {% if not loop.last %},{% endif %}
        {% endfor-%}
    from payments
    where status = "success"
    group by order_id
)



select * from final
