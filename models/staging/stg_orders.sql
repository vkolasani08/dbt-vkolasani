select
--from raw_orders
orderid,
orderdate,
shipdate,
shipmode,
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
{{markup('ORDERSELLIINGPRICE','ordercostprice')}} as markup,
ORDERSELLIINGPRICE - ordercostprice as orderprofit
from {{ ref('raw_orders') }} as o
left join {{ ref('raw_customer') }} as c
on o.customerid = c.customerid
left join {{ ref('raw_product') }} as p
on o.productid = p.productid
--{{limit_data_in_dev('orderdate')}}