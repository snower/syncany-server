
insert into product_order_statistics
select c.name, b.create_time, count(distinct b.member_id) as member_cnt, count(distinct a.order_id) as order_cnt, sum(a.promotion_amount + a.coupon_amount + a.integration_amount + a.real_amount) as total_amount, sum(a.real_amount) as pay_amount
from `data\oms_order_item.csv`a
left join `data\oms_order.csv` b on a.order_id=b.id
left join `data\pms_product.csv` c on a.product_id=c.id
where b.create_time>='${@create_time__gte:1970-01-01 00:00:00}' and b.create_time<'${@create_time__lt:now()}'
group by a.product_id order by order_cnt desc limit 50;