create table dwd.dim_aml_rule(
    rule_sk bigint comment '代理键',
    rule_id string comment '业务主键',
    rule_name string comment '规则名称',
    rule_type string comment '规则类型',-- 转换值：金额/频率/模式/行为
    rule_category string comment '规则类别', -- 转换值：现金/转账/跨境/赌博
    severity_level string comment '严重等级', -- 转换值：低/中/高/严重
    active_status string comment '启用状态',-- 转换值：是/否
    threshold double comment '阈值金额'
)  COMMENT '监控规则维度表'
 PARTITIONED BY (etl_date STRING)
 STORED AS ORC;

insert overwrite table dwd.dim_aml_rule partition (etl_date='2025-08-27')
select
    row_number() over(order by rule_id) + 400000 as rule_sk,
    rule_id,
    rule_name,
    -- 规则类型转换
   case  rule_type
        when 'AMOUNT' then '金额'
        when 'FREQUENCY' then '频率'
        when 'PATTERN' then '模式'
        when 'BEHAVIOR' then '行为'
        else rule_type end as rule_type,
    -- 规则类别转换
    case  rule_category
        when 'CASH' then '现金'
        when 'TFR' then '转账'
        when 'CROSS_BORDER' then '跨境'
        when 'GAMBLING' then '赌博'
        else rule_category end as rule_category,
    -- 严重等级转换
    case  severity_level
        when 'LOW' then '低'
        when 'MEDIUM' then '中'
        when 'HIGH' then '高'
        when 'CRITICAL' then '严重'
        else severity_level end as severity_level,
    -- 启用状态转换
    case active
        when 'Y' then '是'
        when 'N' then '否'
        else active end as active_status,
    -- 从json解析阀值
   cast( get_json_object(param_json,'$.threshold')as double )as threshold
from ods.ods_aml_monitoring_rule;
