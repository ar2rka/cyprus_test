CREATE TABLE bank.tb_daily_agg (
    login int,
    date_from date,
    daily_sum money,
    calculation_ts timestamp,
    constraint c_primary primary key (login, date_from, calculation_ts)
)
ORDER BY login
SEGMENTED BY HASH(login) ALL NODES
PARTITION BY date_from;