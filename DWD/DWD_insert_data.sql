-- 设置hive分区非严格模式
SET hive.exec.dynamic.partition.mode = nonstrict;

-- 向DWD层客户维度表写入数据
INSERT
    OVERWRITE TABLE aml_dwd.dim_aml_customer PARTITION (etl_date)
SELECT
    -- 生成代理键
    row_number() OVER (
        ORDER BY oacm.customer_id
    ) + 100000 AS customer_sk,
    oacm.customer_id AS customer_id,
    oacm.cust_no AS cust_no,
    oacm.cust_name AS cust_name,
    -- 转换客户类型
    CASE oacm.cust_type
        WHEN 'IND' THEN '个人'
        WHEN 'CORP' THEN '对公'
        ELSE '未知'
    END AS cust_type,
    -- 转换证件类型
    CASE oacm.id_type
        WHEN 'ID_CARD' THEN '身份证'
        WHEN 'PASSPORT' THEN '护照'
        WHEN 'BUS_LIC' THEN '营业执照'
        ELSE oacm.id_type
    END AS id_type,
    -- 转换风险等级
    CASE oacm.risk_level
        WHEN 'LOW' THEN '低'
        WHEN 'MEDIUM' THEN '中'
        WHEN 'HIGH' THEN '高'
        WHEN 'VERY_HIGH' THEN '极高'
        ELSE '未知'
    END AS risk_level,
    -- 转换风险来源
    CASE oacm.risk_level_source
        WHEN 'MANUAL' THEN '人工'
        WHEN 'AUTO' THEN '系统自动'
        ELSE oacm.risk_level_source
    END AS risk_level_source,
    -- 转换客户状态
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
    -- 设置失效日期
    coalesce(
        oacm.close_date,
        cast('9999-12-31' AS DATE)
    ) AS end_date,
    -- 设置是否当前版本
    CASE oacm.close_date
        WHEN NULL THEN TRUE
        ELSE FALSE
    END AS is_current,
    -- 设置开链时间
    CURRENT_DATE () AS start_time,
    -- 设置失效时间
    cast('9999-12-31' AS DATE) AS end_time,
    -- 获取分区日期
    oacm.etl_date AS etl_date
FROM aml_ods.ods_aml_customer_master oacm;

-- 向DWD层账户维度表写入数据
INSERT
    OVERWRITE TABLE aml_dwd.dim_aml_account PARTITION (etl_date)
SELECT
    -- 生成代理键
    row_number() OVER (
        ORDER BY account_id
    ) + 200000 AS account_sk,
    account_id,
    customer_id,
    acct_no,
    -- 转换账户类型
    CASE acct_type
        WHEN 'SAVING' THEN '储蓄'
        WHEN 'CURRENT' THEN '活期'
        WHEN 'CREDIT' THEN '信用卡'
        WHEN 'LOAN' THEN '贷款'
        WHEN 'INVESTMENT' THEN '投资'
        ELSE acct_type
    END AS acct_type,
    currency AS currency_code,
    current_balance,
    avg_daily_balance,
    -- 转换账户状态
    CASE status
        WHEN 'ACTIVE' THEN '活跃'
        WHEN 'INACTIVE' THEN '休眠'
        WHEN 'CLOSED' THEN '已销户'
        WHEN 'FROZEN' THEN '冻结'
        ELSE status
    END AS status,
    branch_code,
    -- 转换开户渠道
    CASE channel_open
        WHEN 'COUNTER' THEN '柜面'
        WHEN 'ONLINE' THEN '网银'
        WHEN 'MOBILE' THEN '手机银行'
        ELSE channel_open
    END AS channel_open,
    open_date AS start_date,
    -- 设置失效日期
    CASE
        WHEN close_date IS NULL THEN cast('9999-12-31' AS DATE)
        ELSE close_date
    END AS end_date,
    etl_date
FROM aml_ods.ods_aml_account_master;

-- 向DWD时间维度表写入数据
INSERT
    OVERWRITE TABLE aml_dwd.dim_aml_date
SELECT
    full_date,
    CASE date_format(full_date, 'u')
        WHEN '1' THEN '星期一'
        WHEN '2' THEN '星期二'
        WHEN '3' THEN '星期三'
        WHEN '4' THEN '星期四'
        WHEN '5' THEN '星期五'
        WHEN '6' THEN '星期六'
        WHEN '7' THEN '星期日'
    END AS day_of_week,
    DAY(full_date) AS day_of_month,
    WEEKOFYEAR(full_date) AS week_of_year,
    CASE MONTH(full_date)
        WHEN 1 THEN '一月'
        WHEN 2 THEN '二月'
        WHEN 3 THEN '三月'
        WHEN 4 THEN '四月'
        WHEN 5 THEN '五月'
        WHEN 6 THEN '六月'
        WHEN 7 THEN '七月'
        WHEN 8 THEN '八月'
        WHEN 9 THEN '九月'
        WHEN 10 THEN '十月'
        WHEN 11 THEN '十一月'
        WHEN 12 THEN '十二月'
    END AS month_name,
    QUARTER(full_date) AS QUARTER,
    YEAR(full_date) AS YEAR,
    date_format(full_date, 'u') IN ('6', '7') AS is_weekend,
    CONCAT(
        'FY',
        YEAR(full_date),
        'Q',
        QUARTER(full_date)
    ) AS fiscal_period
FROM (
        SELECT date_add('2000-01-01', pos) AS full_date
        FROM (
                SELECT posexplode(split(space(10957), ' ')) AS (pos, dummy)
            ) t
    ) dates;

INSERT
    OVERWRITE TABLE AML_DWD.DIM_AML_WATCHLIST PARTITION (etl_date)
SELECT
    ROW_NUMBER() OVER () + 300000 AS watchlist_sk,
    oawd.entity_id AS entity_id,
    oawm.list_code AS list_code,
    CASE oawm.list_type
        WHEN 'SANCTIONS' THEN '制裁'
        WHEN 'TERRORIST' THEN '恐怖'
        WHEN 'CRIMINAL' THEN '犯罪'
        WHEN 'PEP' THEN '政要'
        ELSE '其他'
    END AS list_type,
    CASE oawm.list_source
        WHEN 'UN' THEN '联合国'
        WHEN 'OFAC' THEN '美国财务部'
        WHEN 'MPS' THEN '中国公安部'
        WHEN 'INTERNAL' THEN '内部'
        ELSE oawm.list_source
    END AS list_source,
    CASE oawd.entity_type
        WHEN 'PERSON' THEN '个人'
        WHEN 'ORG' THEN '组织'
        ELSE oawd.entity_type
    END AS entity_type,
    CASE oawd.sanction_type
        WHEN 'ASSET_FREEZE' THEN '资产冻结'
        WHEN 'TRAVEL_BAN' THEN '旅行禁令'
        WHEN 'TRADE_EMBARGO' THEN '贸易禁运'
        WHEN 'FINANCIAL_SANCTIONS' THEN '金融制裁'
        ELSE oawd.sanction_type
    END AS sanction_type,
    oawd.nationality AS country_code,
    CASE
        WHEN oawm.list_type IN ('SANCTIONS' 'TERRORIST') THEN '极高'
        WHEN oawm.list_type = 'PEP' THEN '高'
        ELSE '中'
    END AS risk_level,
    oawd.etl_date
FROM aml_ods.ods_aml_watchlist_detail oawd
    JOIN aml_ods.ods_aml_watchlist_master oawm ON oawd.list_id = oawm.list_id;

-- 向DWD层监控规则维度表写入数据
INSERT
    OVERWRITE TABLE aml_dwd.dim_aml_rule PARTITION (etl_date)
SELECT
    -- 生成代理键
    row_number() OVER (
        ORDER BY rule_id
    ) + 400000 AS rule_sk,
    rule_id,
    rule_name,
    -- 转换规则类型
    CASE rule_type
        WHEN 'AMOUNT' THEN '金额'
        WHEN 'FREQUENCY' THEN '频率'
        WHEN 'PATTERN' THEN '模式'
        WHEN 'BEHAVIOR' THEN '行为'
        ELSE rule_type
    END AS rule_type,
    -- 转换规则类别
    CASE rule_category
        WHEN 'CASH' THEN '现金'
        WHEN 'TFR' THEN '转账'
        WHEN 'CROSS_BORDER' THEN '跨境'
        WHEN 'GAMBLING' THEN '赌博'
        ELSE rule_category
    END AS rule_category,
    -- 转换严重等级
    CASE severity_level
        WHEN 'LOW' THEN '低'
        WHEN 'MEDIUM' THEN '中'
        WHEN 'HIGH' THEN '高'
        WHEN 'CRITICAL' THEN '严重'
        ELSE severity_level
    END AS severity_level,
    -- 转换启用状态
    CASE active
        WHEN 'Y' THEN '是'
        WHEN 'N' THEN '否'
        ELSE active
    END AS active_status,
    -- 从JSON中获取阈值金额
    cast(
        get_json_object(param_json, '$.threshold') AS double
    ) AS threshold,
    etl_date
FROM aml_ods.ods_aml_monitoring_rule;