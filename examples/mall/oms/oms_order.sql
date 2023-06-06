
insert into oms_order
SELECT `id[int]`,`member_id[int]`,order_sn,`create_time[datetime]`,`total_amount[double]`,`pay_amount[double]`,`freight_amount[double]`,
       `integration_amount[double]`,`coupon_amount[double]`,`discount_amount[double]`,
       `pay_type[int]`,`source_type[int]`,`status[int]`,`order_type[int]`,delivery_company,delivery_sn,receiver_name,receiver_phone,receiver_province,
       receiver_city,receiver_region,receiver_detail_address,`confirm_status[int]`,`delete_status[int]`,`payment_time[datetime]`,
       `delivery_time[datetime]`,`receive_time[datetime]`,`modify_time[datetime]`
FROM `data/oms_order.csv`;