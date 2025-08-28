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