-- Создание схемы DM
CREATE SCHEMA IF NOT EXISTS dm;

-- Витрина оборотов по лицевым счетам
CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk)
);

-- Витрина остатков по лицевым счетам
CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    balance_out NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk)
);