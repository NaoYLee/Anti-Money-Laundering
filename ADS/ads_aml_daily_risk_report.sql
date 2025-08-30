CREATE TABLE aml_ads.ads_aml_daily_risk_report (
    report_date DATE COMMENT '报告日期',
    txn_type STRING COMMENT '交易类型',
    total_txn_cnt int COMMENT '交易数量',
    high_risk_txn_cnt int COMMENT '高风险交易数量',
    cross_border_ratio DECIMAL(5, 4) COMMENT '跨境交易额比例',
    night_txn_ratio DECIMAL(5, 4) COMMENT '夜间交易额比例'
) COMMENT '交易风险日报' PARTITIONED BY (Generation_date DATE)
STORED AS PARQUET TBLPROPERTIES ("PARQUET.compress" = "SNAPPY");

CREATE VIEW aml_dws.txnType AS
SELECT
    fat.transaction_sk AS transaction_sk,
    CASE
        WHEN fat.txn_type IN ('现金存入', '现金取款') THEN 'incash'
        WHEN fat.txn_type IN ('转入', '转出', '支付')
        AND NOT fat.is_cross_border THEN 'transfer'
        WHEN fat.is_cross_border THEN 'overborder'
    END AS txn_type
FROM aml_dwd.fact_aml_transaction fat
WHERE
    to_date(fat.txn_time) = date_sub(CURRENT_DATE (), 1);
    
SET hive.exec.dynamic.partition.mode = nonstrict;
INSERT INTO
    aml_ads.ads_aml_daily_risk_report PARTITION (Generation_date)
SELECT
    date_sub(CURRENT_DATE, 1) AS report_date,
    CASE tt.txn_type
        WHEN 'incash' THEN '现金'
        WHEN 'transfer' THEN '转账'
        WHEN 'overborder' THEN '跨境'
    END AS txn_type,
    count(DISTINCT fat.transaction_sk) AS total_txn_cnt,
    COUNT(
        DISTINCT CASE
            WHEN fat.amount > 50000
            OR fat.txn_channel = 'ATM' THEN 1
        END
    ) AS high_risk_txn_cnt,
    SUM(
        CASE
            WHEN fat.is_cross_border THEN fat.amount
        END
    ) / SUM(fat.amount) AS cross_border_ratio,
    COUNT(
        CASE
            WHEN HOUR(fat.txn_time) BETWEEN 22 AND 23
            OR HOUR(fat.txn_time) BETWEEN 0 AND 6  THEN 1
        END
    ) / COUNT(*),
    CURRENT_DATE () AS Generation_date
FROM aml_dwd.fact_aml_transaction fat
    JOIN aml_dws.txnType tt ON fat.transaction_sk = tt.transaction_sk
GROUP BY
    tt.txn_type;