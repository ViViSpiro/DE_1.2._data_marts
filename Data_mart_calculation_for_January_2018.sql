-- Расчет витрин за январь 2018 года
DO $$
DECLARE
    calc_date DATE := '2018-01-01';
BEGIN
    WHILE calc_date <= '2018-01-31' LOOP
        -- Расчет витрины оборотов
        CALL ds.fill_account_turnover_f(calc_date);
        
        -- Расчет витрины остатков
        CALL ds.fill_account_balance_f(calc_date);
        
        calc_date := calc_date + INTERVAL '1 day';
    END LOOP;
END;
$$;