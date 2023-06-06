
insert into pms_brand
SELECT `id[int]`,name,first_letter,`factory_status[int]`,logo,brand_story
FROM `data/pms_brand.csv`;