select
--from raw_orders
{{ dbt_utils.surrogate_key(['o.orderid','c.customerid','p.productid']) }} as sk_orders,
orderid,
orderdate,
shipdate,
o.shipmode,
o.customerid,
o.productid,
o.ORDERSELLIINGPRICE,
o.ordercostprice,
--from raw_customer
customername,
segment,
country,
--from raw_product
category,
productname,
subcategory,
d.delivery_team as deliveryteam,
{{markup('ORDERSELLIINGPRICE','ordercostprice')}} as markup,
ORDERSELLIINGPRICE - ordercostprice as orderprofit
from {{ ref('raw_orders') }} as o
left join {{ ref('raw_customer') }} as c
on o.customerid = c.customerid
left join {{ ref('raw_product') }} as p
on o.productid = p.productid
left join {{ ref('delivery_team') }} as d 
on o.shipmode = d.shipmode
