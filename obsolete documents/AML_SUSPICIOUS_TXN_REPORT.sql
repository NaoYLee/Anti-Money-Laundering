CREATE TABLE IF NOT EXISTS ODS.ODS_AML_SUSPICIOUS_TXN_REPORT (
   STR_ID              bigint comment '报告唯一标识ID',
   STR_NO              string comment '报告编号 (业务主键)',
   CUSTOMER_ID         bigint comment '涉及客户ID（外键）',
   REPORT_DATE         DATE comment '报告生成日期',
   SUBMIT_DATE         DATE comment '提交监管机构日期',
   REPORT_STATUS       string comment '报告状态（DRAFT：草稿， SUBMITTED：已提交， WITHDRAWN：已撤回）',
   REPORT_TYPE         string comment '报告类型 (INITIAL：初始， AMENDMENT：修正， CLOSURE：结案）',
   CASE_CATEGORY       string comment '案件类别',
   SUSPICION_BASIS     string comment '怀疑依据（详细描述）',
   TOTAL_AMOUNT        bigint comment '涉及总金额',
   CURRENCY            string comment '涉及币种',
   FIRST_TXN_DATE      DATE comment '首笔可疑交易日期',
   LAST_TXN_DATE       DATE comment '末笔可疑交易日期',
   RELATED_ALERT_IDS   string comment '关联的预警ID列表（逗号分隔）',
   FILED_BY            string comment '填报人',
   FILED_DATE          DATE comment '填报日期',
   APPROVED_BY         string comment '批准人',
   APPROVED_DATE       DATE comment ' 批准日期',
   REGULATOR_REF_NO    string comment '监管机构接收编号',
   COMMENTS            string comment '备注'
)comment '可疑交易报告表'
    partitioned by (etl_date string)
    stored as ORC
    location '/aml/ods/ODS_AML_SUSPICIOUS_TXN_REPORT'
    TBLPROPERTIES ("orc.compress"="SNAPPY") ;