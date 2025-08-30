CREATE DATABASE IF NOT EXISTS AML_ADS COMMENT '反洗钱ADS层数据库' LOCATION '/user/hive/warehouse/AML_ADS.db';

CREATE TABLE aml_ads.ads_aml_customer_risk_profile (
    customer_id bigint COMMENT '客户ID',
    risk_level STRING COMMENT '昨日最新风险等级',
    total_balance decimal(18, 2) COMMENT '账户总余额',
    last_7d_txn_cnt int COMMENT '近7天交易笔数',
    risk_change_flag STRING COMMENT '风险等级变化标志',
    industry_risk_tag STRING COMMENT '行业风险标签'
) COMMENT '客户风险全景视图' PARTITIONED BY (edit_date STRING)
STORED AS PARQUET TBLPROPERTIES ("PARQUET.compress" = "SNAPPY");

-- 将风险等级转化为数字格式
CREATE VIEW riskFlag AS
SELECT
    dacrp.customer_sk,
    CURRENT_DATE () AS today_date,
    dacrp.risk_level AS today_risk_level,
    LAG(dacrp.risk_level, 1) OVER (
        PARTITION BY
            dacrp.customer_sk
        ORDER BY dacrp.etl_date
    ) AS yesterday_risk_level,
    CASE dacrp.risk_level
        WHEN '低' THEN 1
        WHEN '中' THEN 2
        WHEN '高' THEN 3
        WHEN '极高' THEN 4
        ELSE 0
    END AS today_risk_num,
    CASE LAG(dacrp.risk_level, 1) OVER (
            PARTITION BY
                dacrp.customer_sk
            ORDER BY dacrp.etl_date
        )
        WHEN '低' THEN 1
        WHEN '中' THEN 2
        WHEN '高' THEN 3
        WHEN '极高' THEN 4
        ELSE 0
    END AS yesterday_risk_num,
    dacrp.etl_date
FROM aml_dws.dws_aml_customer_risk_profile dacrp;

-- 比较风险等级变化
CREATE VIEW riskCompare AS
SELECT
    customer_sk,
    today_risk_level,
    yesterday_risk_level,
    CASE
        WHEN today_risk_num > yesterday_risk_num THEN '升级'
        WHEN today_risk_num < yesterday_risk_num THEN '降级'
        ELSE '持平'
    END AS risk_change_flag
FROM riskflag;

-- 统计最近七日交易笔数
CREATE VIEW transactionCountLast7d AS
SELECT customer_sk, count(transaction_sk) AS last_7d_txn_cnt
FROM aml_dwd.fact_aml_transaction
WHERE
    date_sk BETWEEN date_sub(CURRENT_DATE (), 7) AND CURRENT_DATE  ()
GROUP BY
    customer_sk;

-- 每天执行一次以下语句，将每日最新数据插入客户风险全景视图中
INSERT
    OVERWRITE TABLE aml_ads.ads_aml_customer_risk_profile PARTITION (etl_date = CURRENT_DATE ())
SELECT
    dacrp.customer_sk AS customer_id,
    rc.yesterday_risk_level AS risk_level,
    dacrp.total_balance AS total_balance,
    tcl.last_7d_txn_cnt AS last_7d_txn_cnt,
    rc.risk_change_flag AS risk_change_flag,
    CASE dacrp.industry_risk_tag
        WHEN TRUE THEN '高危'
        ELSE '常规'
    END AS industry_risk_tag
FROM aml_dws.dws_aml_customer_risk_profile dacrp
    LEFT JOIN aml_ads.riskCompare rc ON dacrp.customer_sk = rc.customer_sk
    LEFT JOIN aml_ads.transactionCountLast7d tcl ON dacrp.customer_sk = tcl.customer_sk
WHERE
    rc.yesterday_risk_level IS NOT NULL;