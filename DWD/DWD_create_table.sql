CREATE DATABASE IF NOT EXISTS AML_DWD COMMENT '反洗钱DWD层数据库' LOCATION '/user/hive/warehouse/AML_DWD.db';

CREATE TABLE aml_dwd.dim_aml_customer (
    customer_sk BIGINT COMMENT '代理键',
    customer_id BIGINT COMMENT '业务主键',
    cust_no STRING COMMENT '客户编号',
    cust_name STRING COMMENT '客户名称',
    cust_type STRING COMMENT '客户类型', -- 转换值：个人/对公
    id_type STRING COMMENT '证件类型', -- 转换值：身份证/护照/营业执照
    risk_level STRING COMMENT '风险等级', -- 转换值：低/中/高/极高
    risk_level_source STRING COMMENT '风险来源', -- 转换值：人工/系统自动
    status STRING COMMENT '状态', -- 转换值：活跃/休眠/已销户/冻结
    industry_type STRING COMMENT '行业类型',
    occupation_type STRING COMMENT '职业类型',
    country_code STRING COMMENT '国籍代码',
    city_code STRING COMMENT '城市代码',
    start_date DATE COMMENT '生效日期',
    end_date DATE COMMENT '失效日期',
    is_current BOOLEAN COMMENT '是否当前版本',
    start_time DATE COMMENT '开链时间',
    end_time DATE COMMENT '开链时间'
) COMMENT '客户维度表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;

SELECT
    row_number() OVER () + 100000 AS customer_sk,
    oacm.customer_id AS customer_id,
    oacm.cust_no AS cust_no,
    oacm.cust_name AS cust_name,
    CASE oacm.cust_type
        WHEN 'IND' THEN '个人'
        WHEN 'CORP' THEN '对公'
        ELSE '未知'
    END AS cust_type,
    CASE oacm.id_type
        WHEN 'ID_CARD' THEN '身份证'
        WHEN 'PASSPORT' THEN '护照'
        WHEN 'BUS_LIC' THEN '营业执照'
        ELSE oacm.id_type
    END AS id_type,
    CASE oacm.risk_level
        WHEN 'LOW' THEN '低'
        WHEN 'MEDIUM' THEN '中'
        WHEN 'HIGH' THEN '高'
        WHEN 'VERY_HIGH' THEN '极高'
        ELSE '未知'
    END AS risk_level,
    CASE oacm.risk_level_source
        WHEN 'MANUAL' THEN '人工'
        WHEN 'AUTO' THEN '系统自动'
        ELSE oacm.risk_level_source
    END AS risk_level_source,
    CASE oacm.status
        WHEN 'ACTIVE' THEN '活跃'
        WHEN 'INACTIVE' THEN '休眠'
        WHEN 'CLOSED' THEN '已销户'
        WHEN 'FROZEN' THEN '冻结'
        ELSE oacm.status
    END AS status,
    oacm.industry AS industry_type,
    oacm.occupation AS occupation_type,
    oacm.nationality AS country_code,
    oacm.residence_city AS city_code,
    oacm.open_date AS start_date,
    coalesce(
        oacm.close_date,
        cast('9999-12-31' AS DATE)
    ) AS end_date,
    CASE oacm.close_date
        WHEN NULL THEN TRUE
        ELSE FALSE
    END AS is_current,
    CURRENT_DATE () AS start_time,
    cast('9999-12-31' AS DATE) AS end_time
FROM ods_aml_customer_master oacm;