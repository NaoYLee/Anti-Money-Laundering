CREATE TABLE IF NOT EXISTS dim_aml_customer_temp AS
SELECT
    dac.customer_sk,
    dac.customer_id,
    dac.cust_no,
    dac.cust_name,
    dac.cust_type,
    dac.id_type,
    dac.risk_level,
    dac.risk_level_source,
    dac.status,
    dac.industry_type,
    dac.occupation_type,
    dac.country_code,
    dac.city_code,
    dac.start_date,
    dac.end_date,
    CASE
        WHEN oacmi1.customer_id IS NOT NULL THEN FALSE
        ELSE TRUE
    END AS is_current,
    dac.start_time,
    CASE
        WHEN oacmi1.customer_id IS NOT NULL THEN date_sub(oacmi1.updated_date, 1)
        ELSE dac.end_time
    END AS end_time,
    oacmi1.etl_date
FROM aml_dwd.dim_aml_customer dac
    JOIN aml_ods.ods_aml_customer_master_increment oacmi1 ON dac.customer_id = oacmi1.customer_id
WHERE
    dac.is_current = TRUE
UNION ALL
SELECT
    co.max_customer_sk + row_number() OVER (
        ORDER BY oacmi2.customer_id
    ) AS customer_sk,
    oacmi2.customer_id AS customer_id,
    oacmi2.cust_no AS cust_no,
    oacmi2.cust_name AS cust_name,
    CASE oacmi2.cust_type
        WHEN 'IND' THEN '个人'
        WHEN 'CORP' THEN '对公'
        ELSE '未知'
    END AS cust_type,
    CASE oacmi2.id_type
        WHEN 'ID_CARD' THEN '身份证'
        WHEN 'PASSPORT' THEN '护照'
        WHEN 'BUS_LIC' THEN '营业执照'
        ELSE oacmi2.id_type
    END AS id_type,
    CASE oacmi2.risk_level
        WHEN 'LOW' THEN '低'
        WHEN 'MEDIUM' THEN '中'
        WHEN 'HIGH' THEN '高'
        WHEN 'VERY_HIGH' THEN '极高'
        ELSE '未知'
    END AS risk_level,
    CASE oacmi2.risk_level_source
        WHEN 'MANUAL' THEN '人工'
        WHEN 'AUTO' THEN '系统自动'
        ELSE oacmi2.risk_level_source
    END AS risk_level_source,
    CASE oacmi2.status
        WHEN 'ACTIVE' THEN '活跃'
        WHEN 'INACTIVE' THEN '休眠'
        WHEN 'CLOSED' THEN '已销户'
        WHEN 'FROZEN' THEN '冻结'
        ELSE oacmi2.status
    END AS status,
    oacmi2.industry AS industry_type,
    oacmi2.occupation AS occupation_type,
    oacmi2.nationality AS country_code,
    oacmi2.residence_city AS city_code,
    oacmi2.open_date AS start_date,
    coalesce(
        oacmi2.close_date,
        cast('9999-12-31' AS DATE)
    ) AS end_date,
    TRUE AS is_current,
    oacmi2.updated_date AS start_time,
    cast('9999-12-31' AS DATE) AS end_time,
    oacmi2.etl_date AS etl_date
FROM aml_ods.ods_aml_customer_master_increment oacmi2
    CROSS JOIN (
        SELECT max(customer_sk) AS max_customer_sk
        FROM aml_dwd.dim_aml_customer
    ) co
WHERE
    oacmi2.open_date < oacmi2.close_date
    OR oacmi2.close_date IS NULL
UNION ALL
SELECT *
FROM aml_dwd.dim_aml_customer
WHERE
    is_current = FALSE;

TRUNCATE TABLE dim_aml_customer;

INSERT INTO
    dim_aml_customer
SELECT *
FROM dim_aml_customer_temp;

TRUNCATE TABLE dim_aml_customer_temp;