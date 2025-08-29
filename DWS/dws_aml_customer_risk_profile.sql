CREATE DATABASE IF NOT EXISTS AML_DWS COMMENT '反洗钱DWS层数据库' LOCATION '/user/hive/warehouse/AML_DWS.db';

CREATE TABLE aml_dws.dws_aml_customer_risk_profile (
    customer_sk BIGINT COMMENT '客户代理键',
    risk_level STRING COMMENT '当前风险等级',
    total_balance DECIMAL(18, 2) COMMENT '总资产余额',
    last_txn_date DATE COMMENT '最后交易日期',
    alert_count INT COMMENT '预警次数',
    screening_hit_count INT COMMENT '名单匹配次数',
    str_submitted BOOLEAN COMMENT '是否提交可疑报告',
    industry_risk_flag BOOLEAN COMMENT '高危行业标识'
) COMMENT '客户风险画像宽表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;

SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO
    aml_dws.dws_aml_customer_risk_profile PARTITION (etl_date);

SELECT
    dac.customer_sk AS customer_sk,
    dac.risk_level AS risk_level,
    sum(
        coalesce(daa.current_balance, 0)
    ) AS total_balance,
    DATE_FORMAT(
        TO_DATE(
            CAST(max(fat.date_sk) AS STRING)
        ),
        'yyyy-MM-dd'
    ) AS last_txn_date count(DISTINCT faa.alert_sk) AS alert_count,
    count(DISTINCT fas.screening_sk) AS screening_hit_count,
    CASE
        WHEN count(report_sk) > 0 THEN TRUE
        ELSE FALSE
    END AS str_submitted,
    CASE
        WHEN coalesce(dac.industry_type, '个人账户') IN (
            '住宿和餐饮业',
            '批发和零售业',
            '房地产业',
            '文化体育娱乐业',
            '建筑业',
            '采矿业',
            '租赁和商务服务业'
        ) THEN TRUE
        ELSE FALSE
    END AS industry_risk_flag,
    dac.etl_date
FROM
    aml_dwd.dim_aml_customer dac
    FULL JOIN aml_dwd.dim_aml_account daa ON dac.customer_id = daa.customer_id
    FULL JOIN aml_dwd.fact_aml_alert faa ON dac.customer_sk = faa.customer_sk
    FULL JOIN aml_dwd.fact_aml_screening fas ON dac.customer_sk = fas.customer_sk
    FULL JOIN aml_dwd.fact_aml_str_report fasr ON dac.customer_sk = fasr.customer_sk
    FULL JOIN aml_dwd.fact_aml_transaction fat ON dac.customer_sk = fat.customer_sk
GROUP BY
    dac.customer_sk,
    dac.risk_level,
    dac.industry_type,
    dac.etl_date;