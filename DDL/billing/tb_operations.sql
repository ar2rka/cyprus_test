CREATE TABLE billing.tb_operations (
    operation_uid int,
    operation_type varchar(10),
    operation_date timestamp,
    login int,
    amount money,
    constraint c_primary primary key (operation_uid)
)
ORDER BY operation_uid, login
SEGMENTED BY HASH(operation_uid) ALL NODES;

CREATE PROJECTION tb_operations_join_optimized (
    operation_uid,
    operation_type,
    operation_date,
    login,
    amount
) AS
SELECT
    operation_uid,
    operation_type,
    operation_date,
    login,
    amount
FROM billing.tb_operations
ORDER BY operation_uid, login
SEGMENTED BY HASH(login) ALL NODES;