CREATE TABLE aml_dws.dws_aml_risk_dashboard (
    date_sk INT COMMENT '日期代理键',
    new_customer_count INT COMMENT '新增客户数',
    high_risk_customer_count INT COMMENT '高风险客户数',
    alert_count INT COMMENT '预警事件数',
    confirmed_alert_count INT COMMENT '确认预警数',
    str_submitted_count INT COMMENT '提交报告数',
    cross_border_txn_count INT COMMENT '跨境交易笔数',
    total_txn_count INT COMMENT '总交易笔数'
) COMMENT '风险监控仪表宽表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;

SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO
    aml_dws.dws_aml_risk_dashboard PARTITION (etl_date)
SELECT
    dad.date_sk AS date_sk,
    COUNT(
        DISTINCT CASE
            WHEN dac.start_date = full_date THEN dac.customer_sk
        END
    ) AS new_customer_count,
    COUNT(
        DISTINCT CASE
            WHEN dac.risk_level IN ('高', '极高') THEN dac.customer_sk
        END
    ) AS high_risk_customer_count,
    COUNT(DISTINCT faa.alert_sk) AS alert_count,
    COUNT(
        DISTINCT CASE
            WHEN faa.alert_status = '确认' THEN faa.alert_sk
        END
    ) AS confirmed_alert_count,
    COUNT(DISTINCT fasr.report_sk) AS str_submitted_count,
    COUNT(
        DISTINCT CASE
            WHEN fat.is_cross_border THEN fat.transaction_sk
        END
    ) AS cross_border_txn_count,
    COUNT(DISTINCT fat.transaction_sk) AS total_txn_count,
    dac.etl_date AS etl_date
FROM
    aml_dwd.dim_aml_customer dac
    FULL JOIN aml_dwd.dim_aml_account daa ON dac.customer_id = daa.customer_id
    FULL JOIN aml_dwd.fact_aml_alert faa ON dac.customer_sk = faa.customer_sk
    FULL JOIN aml_dwd.fact_aml_str_report fasr ON dac.customer_sk = fasr.customer_sk
    FULL JOIN aml_dwd.fact_aml_transaction fat ON dac.customer_sk = fat.customer_sk
    FULL JOIN aml_dwd.dim_aml_date dad ON fat.date_sk = dad.date_sk
GROUP BY
    dad.date_sk, 
    dac.etl_date;