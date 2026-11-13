-- Misma lógica que include/pipelines/pandas_pipeline.py, en SQL declarativo.
select
    c.country,
    strftime(o.order_ts, '%Y-%m')                       as month,
    count(*)                                            as orders,
    sum(o.quantity * o.unit_price * (1 - o.discount))   as revenue,
    avg(o.quantity * o.unit_price * (1 - o.discount))   as avg_ticket
from {{ ref('stg_orders') }} as o
join {{ ref('stg_customers') }} as c using (customer_id)
where o.status = 'completed'
  and o.order_ts >= timestamp '2023-01-01'
group by 1, 2
