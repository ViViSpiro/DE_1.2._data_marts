-- Процедура заполнения витрины остатков по лицевым счетам
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_records_processed INTEGER := 0;
    v_status VARCHAR(20) := 'completed';
    v_error_message TEXT := NULL;
BEGIN
    -- Удаляем старые логи для этой даты
    DELETE FROM logs.etl_logs
    WHERE table_name = 'dm.dm_account_balance_f'
      AND DATE(start_time) = i_OnDate;
    -- Записываем новый лог
    INSERT INTO logs.etl_logs (
        table_name,
        start_time,
        status
    ) VALUES (
        'dm.dm_account_balance_f',
        v_start_time,
        'started'
    );
    -- Удаляем старые данные за эту дату в витрине
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;
    BEGIN
        -- Вставляем новые данные
        INSERT INTO dm.dm_account_balance_f (
            on_date,
            account_rk,
            balance_out,
            balance_out_rub
        )
        WITH active_accounts AS (
            SELECT
                a.account_rk,
                a.char_type,
                a.currency_rk
            FROM ds.md_account_d a
            WHERE i_OnDate BETWEEN a.data_actual_date AND COALESCE(a.data_actual_end_date, '9999-12-31')
        ),
        previous_balance AS (
            SELECT
                b.account_rk,
                b.balance_out,
                b.balance_out_rub
            FROM dm.dm_account_balance_f b
            WHERE b.on_date = i_OnDate - INTERVAL '1 day'
        ),
        current_turnover AS (
            SELECT
                t.account_rk,
                t.credit_amount,
                t.credit_amount_rub,
                t.debet_amount,
                t.debet_amount_rub
            FROM dm.dm_account_turnover_f t
            WHERE t.on_date = i_OnDate
        )
        SELECT
            i_OnDate,
            a.account_rk,
            CASE
                WHEN a.char_type = 'A' THEN
                    COALESCE(pb.balance_out, 0) + COALESCE(ct.debet_amount, 0) - COALESCE(ct.credit_amount, 0)
                WHEN a.char_type = 'P' THEN
                    COALESCE(pb.balance_out, 0) - COALESCE(ct.debet_amount, 0) + COALESCE(ct.credit_amount, 0)
                ELSE COALESCE(pb.balance_out, 0)
            END AS balance_out,
            CASE
                WHEN a.char_type = 'A' THEN
                    COALESCE(pb.balance_out_rub, 0) + COALESCE(ct.debet_amount_rub, 0) - COALESCE(ct.credit_amount_rub, 0)
                WHEN a.char_type = 'P' THEN
                    COALESCE(pb.balance_out_rub, 0) - COALESCE(ct.debet_amount_rub, 0) + COALESCE(ct.credit_amount_rub, 0)
                ELSE COALESCE(pb.balance_out_rub, 0)
            END AS balance_out_rub
        FROM active_accounts a
        LEFT JOIN previous_balance pb ON a.account_rk = pb.account_rk
        LEFT JOIN current_turnover ct ON a.account_rk = ct.account_rk;
        GET DIAGNOSTICS v_records_processed = ROW_COUNT;
    EXCEPTION WHEN OTHERS THEN
        v_status := 'failed';
        v_error_message := SQLERRM;
        RAISE NOTICE 'Ошибка при заполнении витрины остатков: %', SQLERRM;
    END;
    -- Обновляем лог
    UPDATE logs.etl_logs
    SET
        end_time = CURRENT_TIMESTAMP,
        status = v_status,
        records_processed = v_records_processed,
        error_message = v_error_message
    WHERE
        table_name = 'dm.dm_account_balance_f' AND
        start_time = v_start_time;
    RAISE NOTICE 'Обработано % записей за дату %', v_records_processed, i_OnDate;
END;
$$;