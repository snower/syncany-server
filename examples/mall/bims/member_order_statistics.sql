
insert into member_order_statistics
select b.username, b.nickname, b.phone, a.create_time, count(*) as order_cnt, sum(a.total_amount) as total_amount, sum(a.pay_amount) as pay_amount
from `data\oms_order.csv` a
left join `data\ums_member.csv` b on a.member_id=b.id
where a.create_time>='${@create_time__gte:1970-01-01 00:00:00}' and a.create_time<'${@create_time__lt:now()}'
group by a.member_id order by order_cnt desc limit 50;