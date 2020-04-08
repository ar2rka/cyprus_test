CREATE TABLE orderstat.tb_orders (
    order_uid int,
    login int,
    order_close_date timestamp,
    constraint c_primary primary key (order_uid)
)
ORDER BY order_uid, login
SEGMENTED BY HASH(login) ALL NODES;