-- Процедура заполнения витрины оборотов по лицевым счетам
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_records_processed INTEGER := 0;
    v_status VARCHAR(20) := 'completed';
    v_error_message TEXT := NULL;
BEGIN
    -- Удаляем старые логи на дату расчета, если они есть
    DELETE FROM logs.etl_logs
    WHERE table_name = 'dm.dm_account_turnover_f'
      AND DATE(start_time) = i_OnDate;
    -- Записываем новый лог
    INSERT INTO logs.etl_logs (
        table_name,
        start_time,
        status
    ) VALUES (
        'dm.dm_account_turnover_f',
        v_start_time,
        'started'
    );
    -- Удаляем старые данные на дату расчета в витрине
    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;
    BEGIN
        -- Вставляем новые данные
        INSERT INTO dm.dm_account_turnover_f (
            on_date,
            account_rk,
            credit_amount,
            credit_amount_rub,
            debet_amount,
            debet_amount_rub
        )
        WITH
        all_accounts AS (
            SELECT credit_account_rk AS account_rk FROM ds.ft_posting_f WHERE oper_date = i_OnDate
            UNION
            SELECT debet_account_rk AS account_rk FROM ds.ft_posting_f WHERE oper_date = i_OnDate
        ),
        credit_turn AS (
            SELECT
                p.credit_account_rk AS account_rk,
                COALESCE(SUM(p.credit_amount), 0) AS credit_amount,
                COALESCE(SUM(p.credit_amount * COALESCE(er.reduced_cource, 1)), 0) AS credit_amount_rub
            FROM ds.ft_posting_f p
            LEFT JOIN ds.md_account_d a ON p.credit_account_rk = a.account_rk
            LEFT JOIN ds.md_exchange_rate_d er ON
                a.currency_rk = er.currency_rk AND
                i_OnDate BETWEEN er.data_actual_date AND COALESCE(er.data_actual_end_date, '9999-12-31')
            WHERE p.oper_date = i_OnDate
            GROUP BY p.credit_account_rk
        ),
        debit_turn AS (
            SELECT
                p.debet_account_rk AS account_rk,
                COALESCE(SUM(p.debet_amount), 0) AS debet_amount,
                COALESCE(SUM(p.debet_amount * COALESCE(er.reduced_cource, 1)), 0) AS debet_amount_rub
            FROM ds.ft_posting_f p
            LEFT JOIN ds.md_account_d a ON p.debet_account_rk = a.account_rk
            LEFT JOIN ds.md_exchange_rate_d er ON
                a.currency_rk = er.currency_rk AND
                i_OnDate BETWEEN er.data_actual_date AND COALESCE(er.data_actual_end_date, '9999-12-31')
            WHERE p.oper_date = i_OnDate
            GROUP BY p.debet_account_rk
        )
        SELECT
            i_OnDate,
            a.account_rk,
            COALESCE(ct.credit_amount, 0),
            COALESCE(ct.credit_amount_rub, 0),
            COALESCE(dt.debet_amount, 0),
            COALESCE(dt.debet_amount_rub, 0)
        FROM all_accounts a
        LEFT JOIN credit_turn ct ON a.account_rk = ct.account_rk
        LEFT JOIN debit_turn dt ON a.account_rk = dt.account_rk;
        GET DIAGNOSTICS v_records_processed = ROW_COUNT;
    EXCEPTION WHEN OTHERS THEN
        v_status := 'failed';
        v_error_message := SQLERRM;
        RAISE NOTICE 'Ошибка при заполнении витрины оборотов: %', SQLERRM;
    END;
    -- Обновляем лог
    UPDATE logs.etl_logs
    SET
        end_time = CURRENT_TIMESTAMP,
        status = v_status,
        records_processed = v_records_processed,
        error_message = v_error_message
    WHERE
        table_name = 'dm.dm_account_turnover_f' AND
        start_time = v_start_time;
    RAISE NOTICE 'Обработано % записей за дату %', v_records_processed, i_OnDate;
END;
$$;