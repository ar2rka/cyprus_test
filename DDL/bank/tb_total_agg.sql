CREATE TABLE bank.tb_total_agg (
    login int,
    total_sum money,
    calculation_ts timestamp,
    constraint c_primary primary key (login, calculation_ts)
)
ORDER BY login
SEGMENTED BY HASH(login) ALL NODES;
-- в отличии от других агрегатов, в этой таблице поле calculation_ts необходимо для корректного использования
-- так пользователи будут понимать возможный лаг от факта
