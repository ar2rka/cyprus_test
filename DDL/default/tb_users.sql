create table "default".tb_users (
    uid int,
    registration_date timestamp,
    country varchar(4),
    first_name varchar(80),
    constraint c_primary primary key (uid)
)
ORDER BY uid
SEGMENTED BY HASH(uid) ALL NODES;