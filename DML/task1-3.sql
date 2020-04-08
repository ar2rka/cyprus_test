WITH gte_1k AS (
    SELECT us.uid,
           us.country
    FROM "default".tb_users us
             JOIN "default".tb_logins log ON us.uid = log.user_uid
             JOIN billing.tb_operations op ON log.login = op.login
    WHERE operation_type = 'deposit'
    GROUP BY us.uid, us.country
    HAVING AVG(op.amount) >= 1000
-- выбор клиентов с средним депозитом >= 1000
),
all_clients AS (
    SELECT
        count(uid) as count_all,
        country
    FROM "default".tb_users
    GROUP BY country
-- подсчет всех клиентов по странам
)
SELECT gte_1k.country, count(gte_1k.uid) as avg_dep_gte_1k, count_all
FROM gte_1k
join all_clients on all_clients.country = gte_1k.country
GROUP BY gte_1k.country, count_all
ORDER BY country;

