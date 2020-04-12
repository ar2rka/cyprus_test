WITH op AS (
    SELECT us.uid,
           us.registration_date,
           us.country,
           log.login,
           op.operation_date,
           row_number() over (PARTITION BY uid ORDER BY operation_date ASC) as rn
    FROM "default".tb_users us
    JOIN "default".tb_logins log on us.uid = log.user_uid
    JOIN billing.tb_operations op on log.login = op.login
    WHERE op.operation_type = 'deposit' AND log.account_type = 'real'
-- подготовка для выбора первого внесения депозита пользователем на любой из его счетов
),
ord AS (
    SELECT login, min(order_close_date) as first_order_date
    FROM orderstat.tb_orders
    GROUP BY login
-- выбор первой сделки
)
SELECT
       op.country,
       COUNT(op.login) as login_count,
       AVG(datediff('day', op.registration_date, op.operation_date)) as avg_reg_to_dep, -- единицы разницы легко изменить
       AVG(datediff('day', op.operation_date, ord.first_order_date)) as avg_dep_to_ord -- NULL если не было сделок
FROM op
LEFT JOIN ord ON op.login = ord.login
WHERE op.rn = 1 AND op.registration_date >= timestampadd('day', -900, current_timestamp)
GROUP BY op.country
ORDER BY login_count DESC;