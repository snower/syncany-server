
insert into ums_member
SELECT `id[int]`,username,nickname,phone,icon,`gender[int]`,`birthday[date]`,`status[int]`,`create_time[datetime]`
FROM `data/ums_member.csv`;