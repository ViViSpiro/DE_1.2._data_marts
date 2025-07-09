-- Заполнение начальных остатков на 31.12.2017
INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
SELECT
    b.on_date,
    b.account_rk,
    b.balance_out,
    b.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
FROM ds.ft_balance_f b
LEFT JOIN ds.md_account_d a ON b.account_rk = a.account_rk
LEFT JOIN ds.md_exchange_rate_d er ON
    a.currency_rk = er.currency_rk AND
    b.on_date BETWEEN er.data_actual_date AND COALESCE(er.data_actual_end_date, '9999-12-31')
WHERE b.on_date = '2017-12-31';