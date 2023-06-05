
insert into pms_product_category
SELECT `id[int]`,`parent_id[int]`,name,icon,description
FROM `data\pms_product_category.csv`;