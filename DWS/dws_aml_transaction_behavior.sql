CREATE TABLE aml_dws.dws_aml_transaction_behavior (
    date_sk INT COMMENT '日期代理键',
    txn_type STRING COMMENT '交易类型',
    channel_type STRING COMMENT '渠道类型',
    is_high_risk BOOLEAN COMMENT '是否高风险',
    txn_count INT COMMENT '交易笔数',
    total_amount DECIMAL(18, 2) COMMENT '总交易金额',
    avg_amount DECIMAL(18, 2) COMMENT '平均交易金额',
    cross_border_ratio DECIMAL(5, 2) COMMENT '跨境交易占比'
) COMMENT '交易行为分析宽表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;

SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO
    aml_dws.dws_aml_transaction_behavior PARTITION (etl_date)
SELECT
    a.date_sk,
    b.txn_type,
    CASE b.txn_channel
        WHEN 'ATM' THEN '线下'
        WHEN 'POS' THEN '线下'
        WHEN '柜面' THEN '线下'
        ELSE '线上'
    END AS channel_type,
    b.is_high_risk,
    count(b.transaction_sk) AS txn_count,
    sum(abs(b.amount)) AS total_amount,
    avg(abs(b.amount)) AS avg_amount,
    SUM(
        CASE
            WHEN b.is_cross_border THEN 1
            ELSE 0
        END
    ) / COUNT(*) AS cross_border_ratio,
    b.etl_date AS etl_date
FROM aml_dwd.dim_aml_date AS a
    FULL JOIN aml_dwd.fact_aml_transaction AS b ON a.date_sk = b.date_sk
GROUP BY
    a.date_sk,
    b.txn_type,
    b.txn_channel,
    b.is_high_risk,
    b.etl_date;