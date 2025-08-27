-- ODS层监控规则配置表
CREATE TABLE IF NOT EXISTS ods.ods_aml_monitoring_rule
(
    rule_id            STRING COMMENT '规则唯一ID (业务主键)',
    rule_name          STRING COMMENT '规则名称',
    rule_desc          STRING COMMENT '规则描述',
    rule_type          STRING COMMENT '规则类型 (AMOUNT:金额, FREQUENCY:频率, PATTERN:模式, BEHAVIOR:行为)',
    rule_category      STRING COMMENT '规则类别 (CASH:现金, TFR:转账, CROSS_BORDER:跨境, GAMBLING:赌博)',
    active             STRING COMMENT '是否启用 (Y/N)',
    severity_level     STRING COMMENT '触发后预警严重等级',
    param_json         STRING COMMENT '规则参数 (JSON格式存储)',
    sql_expression     STRING COMMENT '规则对应的SQL表达式 (可选)',
    last_modified_by   STRING COMMENT '最后修改人',
    last_modified_date STRING COMMENT '最后修改日期'
)
    COMMENT '监控规则配置表：存储用于触发预警的业务规则'
    PARTITIONED BY (etl_date STRING COMMENT '数据加载日期')
    STORED AS ORC
    LOCATION '/data/aml/ods/monitoring_rule'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
;

-- ODS层筛查结果表
CREATE TABLE IF NOT EXISTS ods.ods_aml_screening_result
(
    result_id         BIGINT COMMENT '筛查结果唯一标识ID',
    screening_type    STRING COMMENT '筛查类型 (CUSTOMER:客户开户, TRANSACTION:交易, PERIODIC:定期回溯)',
    screening_date    STRING COMMENT '筛查日期',
    screening_time    STRING COMMENT '筛查时间戳',
    target_type       STRING COMMENT '目标类型 (CUSTOMER:客户, ACCT:账户, COUNTERPARTY:交易对手)',
    target_id         BIGINT COMMENT '目标ID (CUSTOMER_ID, ACCOUNT_ID, 或交易对手ID)',
    target_name       STRING COMMENT '目标名称',
    matched_entity_id BIGINT COMMENT '匹配的名单条目ID (外键)',
    matched_list_id   BIGINT COMMENT '匹配的名单ID (外键)',
    match_score       BIGINT COMMENT '匹配度评分 (0-100)',
    match_level       STRING COMMENT '匹配等级 (EXACT:完全匹配, FUZZY_HIGH:模糊高, FUZZY_MED:模糊中, FUZZY_LOW:模糊低)',
    screening_status  STRING COMMENT '筛查状态 (PENDING:待审, ALERT:警报, FALSE_POSITIVE:误报, CONFIRMED:确认)',
    reviewed_by       STRING COMMENT '审核人',
    reviewed_date     STRING COMMENT '审核日期',
    comments          STRING COMMENT '审核意见',
    created_date      STRING COMMENT '创建日期'
) COMMENT '反洗钱筛查结果表'
PARTITIONED BY (etl_date STRING COMMENT '数据加载日期')
    STORED AS ORC
    LOCATION '/data/aml/ods/screening_result'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");