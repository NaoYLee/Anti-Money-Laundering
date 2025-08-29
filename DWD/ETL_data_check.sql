-- 客户维度空值率检查
SELECT
    COUNT(
        CASE
            WHEN customer_sk IS NULL THEN 1
        END
    ) / COUNT(*) AS customer_sk_null_rate,
    COUNT(
        CASE
            WHEN cust_type IS NULL THEN 1
        END
    ) / COUNT(*) AS cust_type_null_rate
FROM aml_dwd.dim_aml_customer;

-- 交易金额异常值检测
SELECT COUNT(
        CASE
            WHEN amount < -1000000
            OR amount > 1000000 THEN 1
        END
    ) AS amount_outlier_count
FROM aml_dwd.fact_aml_transaction;

-- 枚举值有效性检查
SELECT report_status, COUNT(*) AS cnt
FROM aml_dwd.fact_aml_str_report
GROUP BY
    report_status
HAVING
    report_status NOT IN ('草稿', '已提交', '已撤回');

-- ODS与DWD数据量对比
WITH
    ods_count AS (
        SELECT COUNT(*) AS cnt
        FROM aml_ods.ods_aml_transaction_detail
    ),
    dwd_count AS (
        SELECT COUNT(*) AS cnt
        FROM aml_dwd.fact_aml_transaction
    )
SELECT o.cnt AS ods_count, d.cnt AS dwd_count, ABS(o.cnt - d.cnt) AS diff
FROM ods_count o, dwd_count d;

-- 客户维度枚举值检查
SELECT 
    cust_type,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_customer
GROUP BY cust_type
HAVING cust_type NOT IN ('个人', '对公', '未知');

SELECT 
    id_type,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_customer
GROUP BY id_type
HAVING id_type NOT IN ('身份证', '护照', '营业执照');

SELECT 
    risk_level,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_customer
GROUP BY risk_level
HAVING risk_level NOT IN ('低', '中', '高', '极高', '未知');

SELECT 
    status,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_customer
GROUP BY status
HAVING status NOT IN ('活跃', '休眠', '已销户', '冻结');

-- 账户维度枚举值检查
SELECT 
    acct_type,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_account
GROUP BY acct_type
HAVING acct_type NOT IN ('储蓄', '活期', '信用卡', '贷款', '投资');

SELECT 
    status,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_account
GROUP BY status
HAVING status NOT IN ('活跃', '休眠', '已销户', '冻结');

SELECT 
    channel_open,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_account
GROUP BY channel_open
HAVING channel_open NOT IN ('柜面', '网银', '手机银行');

-- 交易事实表枚举值检查
SELECT 
    txn_type,
    COUNT(*) AS cnt
FROM aml_dwd.fact_aml_transaction
GROUP BY txn_type
HAVING txn_type NOT IN ('现金存入', '现金取款', '转入', '转出', '支付');

SELECT 
    txn_channel,
    COUNT(*) AS cnt
FROM aml_dwd.fact_aml_transaction
GROUP BY txn_channel
HAVING txn_channel NOT IN ('柜面', 'ATM', '网银', '手机银行', 'POS');

SELECT 
    txn_status,
    COUNT(*) AS cnt
FROM aml_dwd.fact_aml_transaction
GROUP BY txn_status
HAVING txn_status NOT IN ('成功', '失败', '待处理');

-- 监控规则维度表枚举值检查
SELECT 
    rule_type,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_rule
GROUP BY rule_type
HAVING rule_type NOT IN ('金额', '频率', '模式', '行为');

SELECT 
    rule_category,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_rule
GROUP BY rule_category
HAVING rule_category NOT IN ('现金', '转账', '跨境', '赌博');

SELECT 
    severity_level,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_rule
GROUP BY severity_level
HAVING severity_level NOT IN ('低', '中', '高', '严重');

SELECT 
    active_status,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_rule
GROUP BY active_status
HAVING active_status NOT IN ('是', '否');

-- 预警事实表枚举值检查
SELECT 
    alert_type,
    COUNT(*) AS cnt
FROM aml_dwd.fact_aml_alert
GROUP BY alert_type
HAVING alert_type NOT IN ('规则', '模型');

SELECT 
    alert_status,
    COUNT(*) AS cnt
FROM aml_dwd.fact_aml_alert
GROUP BY alert_status
HAVING alert_status NOT IN ('待处理', '审核中', '排除', '确认');

SELECT 
    severity_level,
    COUNT(*) AS cnt
FROM aml_dwd.fact_aml_alert
GROUP BY severity_level
HAVING severity_level NOT IN ('低', '中', '高', '严重');

-- 可疑交易报告枚举值检查
SELECT 
    report_type,
    COUNT(*) AS cnt
FROM aml_dwd.fact_aml_str_report
GROUP BY report_type
HAVING report_type NOT IN ('初始', '修正', '结案');

SELECT 
    case_category,
    COUNT(*) AS cnt
FROM aml_dwd.fact_aml_str_report
GROUP BY case_category
HAVING case_category NOT IN ('洗钱', '恐怖融资', '欺诈');

-- 日期维度检查
SELECT 
    COUNT(*) AS invalid_date_count
FROM aml_dwd.dim_aml_date
WHERE full_date IS NULL OR date_sk IS NULL;

-- 名单维度表枚举值检查
SELECT 
    list_type,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_watchlist
GROUP BY list_type
HAVING list_type NOT IN ('制裁', '恐怖', '犯罪', '政要', '其他');

SELECT 
    list_source,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_watchlist
GROUP BY list_source
HAVING list_source NOT IN ('联合国', '美国财务部', '中国公安部', '内部');

SELECT 
    entity_type,
    COUNT(*) AS cnt
FROM aml_dwd.dim_aml_watchlist
GROUP BY entity_type
HAVING entity_type NOT IN ('个人', '组织');

-- 检查代理键唯一性
SELECT 
    COUNT(*) - COUNT(DISTINCT customer_sk) AS customer_sk_duplicate_count
FROM aml_dwd.dim_aml_customer;

SELECT 
    COUNT(*) - COUNT(DISTINCT account_sk) AS account_sk_duplicate_count
FROM aml_dwd.dim_aml_account;

SELECT 
    COUNT(*) - COUNT(DISTINCT transaction_sk) AS transaction_sk_duplicate_count
FROM aml_dwd.fact_aml_transaction;

-- 检查关键业务字段完整性
SELECT
    COUNT(
        CASE
            WHEN customer_id IS NULL THEN 1
        END
    ) / COUNT(*) AS customer_id_null_rate,
    COUNT(
        CASE
            WHEN cust_no IS NULL THEN 1
        END
    ) / COUNT(*) AS cust_no_null_rate,
    COUNT(
        CASE
            WHEN cust_name IS NULL THEN 1
        END
    ) / COUNT(*) AS cust_name_null_rate
FROM aml_dwd.dim_aml_customer;

-- 检查金额字段合理性
SELECT 
    COUNT(*) AS negative_balance_count
FROM aml_dwd.dim_aml_account
WHERE current_balance < 0;

SELECT 
    COUNT(*) AS negative_amount_count
FROM aml_dwd.fact_aml_transaction
WHERE amount < 0;

-- 检查日期逻辑一致性
SELECT 
    COUNT(*) AS invalid_date_range_count
FROM aml_dwd.dim_aml_customer
WHERE start_date > end_date;

SELECT 
    COUNT(*) AS invalid_date_range_count
FROM aml_dwd.dim_aml_account
WHERE start_date > end_date;