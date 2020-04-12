CREATE TABLE "default".tb_logins (
    user_uid int,
    login int,
    account_type varchar(8),
    created_ts timestamp,
    constraint c_primary primary key (login)
)
ORDER BY user_uid, login
SEGMENTED BY HASH(login) ALL NODES;

CREATE PROJECTION tb_logins_join_optimized (
    user_uid,
    login,
    account_type
) AS
SELECT
    user_uid,
    login,
    account_type
FROM "default".tb_logins
ORDER BY user_uid, login
SEGMENTED BY HASH(user_uid) ALL NODES;
-- проекция создана для оптимизации запросов с джойном по клиентам