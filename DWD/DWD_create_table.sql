CREATE DATABASE IF NOT EXISTS AML_DWD COMMENT '反洗钱DWD层数据库' LOCATION '/user/hive/warehouse/AML_DWD.db';

-- 创建DWD层客户维度表
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

-- 创建DWD层账户维度表
CREATE TABLE aml_dwd.dim_aml_account (
    account_sk BIGINT COMMENT '代理键',
    account_id BIGINT COMMENT '业务ID',
    customer_id BIGINT COMMENT '客户ID',
    acct_no STRING COMMENT '账户号码',
    acct_type STRING COMMENT '账户类型', -- 转换值：储蓄/活期/信用卡/贷款 / 投资
    currency_code STRING COMMENT '币种代码',
    current_balance DECIMAL(18, 2) COMMENT '当前余额',
    avg_daily_balance DECIMAL(18, 2) COMMENT '日均余额',
    status STRING COMMENT '状态', -- 转换值：活跃/休眠/已销户/冻结
    branch_code STRING COMMENT '网点代码',
    channel_open STRING COMMENT '开户渠道', -- 转换值：柜面/网银/手机银行
    start_date DATE COMMENT '生效日期',
    end_date DATE COMMENT '失效日期'
) COMMENT '账户维度表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;

-- 创建DWD层时间维度表
CREATE TABLE aml_dwd.dim_aml_date (
    date_sk int COMMENT '代理键',
    full_date DATE COMMENT '日期',
    day_of_week STRING COMMENT '星期几',
    day_of_month INT COMMENT '月中第几天',
    week_of_year INT COMMENT '年中第几周',
    month_name STRING COMMENT '月份名称',
    QUARTER INT COMMENT '季度',
    YEAR INT COMMENT '年份',
    is_weekend BOOLEAN COMMENT '是否周末',
    fiscal_period STRING COMMENT '财年期间'
) COMMENT '时间维度表'
STORED AS ORC;

-- 创建DWD层名单维度表
CREATE TABLE aml_dwd.dim_aml_watchlist (
    watchlist_sk BIGINT COMMENT '代理键',
    entity_id BIGINT COMMENT '业务主键',
    list_code STRING COMMENT '名单代码',
    list_type STRING COMMENT '名单类型', -- 转换值：制裁/恐怖/犯罪/政要/其他
    list_source STRING COMMENT '名单来源', -- 转换值：联合国/美国财政部/中国公安部/内部
    entity_type STRING COMMENT '实体类型', -- 转换值：个人/组织
    sanction_type STRING COMMENT '制裁类型', -- 转换值：资产冻结/旅行禁令等
    country_code STRING COMMENT '国籍代码',
    risk_level STRING COMMENT '风险等级'
) COMMENT '名单维度表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;

-- 创建DWD层监控规则维度表
CREATE TABLE aml_dwd.dim_aml_rule (
    rule_sk bigint COMMENT '代理键',
    rule_id STRING COMMENT '业务主键',
    rule_name STRING COMMENT '规则名称',
    rule_type STRING COMMENT '规则类型', -- 转换值：金额/频率/模式/行为
    rule_category STRING COMMENT '规则类别', -- 转换值：现金/转账/跨境/赌博
    severity_level STRING COMMENT '严重等级', -- 转换值：低/中/高/严重
    active_status STRING COMMENT '启用状态', -- 转换值：是/否
    threshold double COMMENT '阈值金额'
) COMMENT '监控规则维度表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;

-- 创建DWD层交易事实表
CREATE TABLE aml_dwd.fact_aml_transaction (
    transaction_sk BIGINT COMMENT '代理键',
    customer_sk BIGINT COMMENT '客户代理键',
    account_sk BIGINT COMMENT '账户代理键',
    date_sk int COMMENT '日期代理键',
    currency_sk STRING COMMENT '币种代理键',
    txn_type STRING COMMENT '交易类型', -- 转换值：现金存入/现金取款/转入/转出/支付
    txn_sub_type STRING COMMENT '交易子类型',
    txn_channel STRING COMMENT '交易渠道', -- 转换值：柜面/ATM/网银/手机银行/POS
    txn_status STRING COMMENT '交易状态', -- 转换值：成功/失败/待处理
    amount DECIMAL(18, 2) COMMENT '交易金额',
    is_cross_border BOOLEAN COMMENT '是否跨境',
    is_high_risk BOOLEAN COMMENT '是否高风险'
) COMMENT '交易事实表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;

-- 创建DWD层名单筛查事实表
CREATE TABLE aml_dwd.fact_aml_screening (
    screening_sk BIGINT COMMENT '代理键',
    customer_sk BIGINT COMMENT '客户代理键',
    watchlist_sk BIGINT COMMENT '名单代理键',
    date_sk int COMMENT '日期代理键',
    screening_type STRING COMMENT '筛查类型', -- 转换值：客户开户/交易/定期回溯
    target_type STRING COMMENT '目标类型', -- 转换值：客户/账户/交易对手
    match_level STRING COMMENT '匹配等级', -- 转换值：完全匹配/模糊高/模糊中/模糊低
    screening_status STRING COMMENT '筛查状态', -- 转换值：待审/警报/误报/确认
    match_score DECIMAL(5, 2) COMMENT '匹配分数'
) COMMENT '名单筛查事实表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;

-- 创建DWD层预警事实表
CREATE TABLE aml_dwd.fact_aml_alert (
    alert_sk BIGINT COMMENT '代理键',
    customer_sk BIGINT COMMENT '客户代理键',
    account_sk BIGINT COMMENT '账户代理键',
    rule_sk BIGINT COMMENT '规则代理键',
    date_sk int COMMENT '日期代理键',
    alert_type STRING COMMENT '预警类型', -- 转换值：规则/模型
    alert_status STRING COMMENT '预警状态', -- 转换值：待处理/审核中/排除/确认
    severity_level STRING COMMENT '严重等级', -- 转换值：低/中/高/严重
    trigger_amount DECIMAL(18, 2) COMMENT '触发金额',
    trigger_txn_count INT COMMENT '触发笔数'
) COMMENT '预警事实表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;

-- 创建DWD层可疑交易报告事实表
CREATE TABLE aml_dwd.fact_aml_str_report (
    report_sk BIGINT COMMENT '代理键',
    customer_sk BIGINT COMMENT '客户代理键',
    date_sk int COMMENT '报告日期代理键',
    first_txn_date_sk int COMMENT '首笔交易日期代理键',
    last_txn_date_sk int COMMENT '末笔交易日期代理键',
    report_type STRING COMMENT '报告类型', -- 转换值：初始/修正/结案
    case_category STRING COMMENT '案件类别', -- 转换值：洗钱/恐怖融资/欺诈
    report_status STRING COMMENT '报告状态', -- 转换值：草稿/已提交/已撤回
    total_amount DECIMAL(18, 2) COMMENT '总金额'
) COMMENT '可疑交易报告事实表' PARTITIONED BY (etl_date STRING)
STORED AS ORC;