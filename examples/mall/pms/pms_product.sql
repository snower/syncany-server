
insert into pms_product
SELECT `id[int]`,`brand_id[int]`,`product_category_id[int]`,name,pic,product_sn,`sale[int]`,`price[double]`,
                               sub_title,description,`original_price[double]`,`stock[int]`,unit,`delete_status[int]`
FROM `data/pms_product.csv`;