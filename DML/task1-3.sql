WITH gte_1k AS (
    SELECT us.uid,
           us.country
    FROM "default".tb_users us
    JOIN "default".tb_logins log ON us.uid = log.user_uid
    JOIN billing.tb_operations op ON log.login = op.login
    WHERE operation_type = 'deposit'
    GROUP BY us.uid, us.country
    HAVING AVG(op.amount) >= 1000
)
SELECT
    us.country,
    count(distinct us.uid) as count_all,
    count(distinct gte_1k.uid) as avg_dep_gte_1k
FROM "default".tb_users us
JOIN gte_1k ON us.country = gte_1k.country
GROUP BY us.country
ORDER BY us.country;