SELECT 
    uid,
    login,
    operation_date,
    dep_rank
FROM (
    SELECT us.uid,
    log.login,
    operation_date,
    rank() over (PARTITION BY uid ORDER BY operation_date) as dep_rank
    FROM "default".tb_users us
    JOIN "default".tb_logins log ON us.uid = log.user_uid
    JOIN billing.tb_operations op ON log.login = op.login
    WHERE operation_type = 'deposit'
-- выбор нужного набора данных и проставление порядкого номера каждому депозиту
) subq
WHERE dep_rank <= 3
ORDER BY uid, dep_rank;
