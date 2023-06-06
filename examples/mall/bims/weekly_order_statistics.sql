
insert into weekly_order_statistics
select YEARWEEK(create_time) as order_week, create_time, count(distinct member_id) as member_cnt, count(*) as order_cnt, sum(total_amount) as total_amount, sum(pay_amount) as pay_amount
from `data/oms_order.csv` where create_time>='${@create_time__gte:1970-01-01 00:00:00}' and create_time<'${@create_time__lt:now()}' group by YEARWEEK(create_time);