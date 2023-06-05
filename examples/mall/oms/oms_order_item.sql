
insert into oms_order_item
SELECT `id[int]`,`order_id[int]`,order_sn,`product_id[int]`,product_pic,product_name,product_brand,product_sn,`product_price[double]`,
       `product_sku_id[int]`,product_sku_code,`product_category_id[int]`,promotion_name,`promotion_amount[double]`,`coupon_amount[double]`,
       `integration_amount[double]`,`real_amount[double]`,product_attr
FROM `data\oms_order_item.csv`;