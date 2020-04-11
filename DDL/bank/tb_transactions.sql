CREATE TABLE bank.tb_transactions (
    transaction_uid int,
    login int NOT NULL,
    counter_login int,
    transaction_type int NOT NULL,
    transaction_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    comment varchar(256),
    amount money,
    constraint c_primary primary key (transaction_uid)
)
ORDER BY transaction_uid
SEGMENTED BY HASH(transaction_uid) ALL NODES
PARTITION BY transaction_ts::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(transaction_ts::DATE, 3, 3);

