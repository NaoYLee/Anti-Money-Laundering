CREATE external TABLE if not exists ODS.ODS_AML_UBO_INFO (
    UBO_ID              BIGINT comment '客户唯一标识ID(主键)',--客户唯一标识ID (主键)
    CUSTOMER_ID         BIGINT comment '关联的对公客户ID',--关联的对公客户ID
    UBO_NAME            STRING comment '受益所有人姓名',--受益所有人姓名
    UBO_ID_TYPE         STRING comment '受益所有人证件类型',--受益所有人证件类型
    UBO_ID_NO           STRING comment '受益所有人证件号码',--受益所有人证件号码
    UBO_SHARE_PERCENT   BIGINT comment '持股比例(%)',--持股比例(%)
    UBO_POSITION         STRING comment '职务(如董事,控股股东)',--职务(如董事,控股股东)
    UBO_NATIONALITY      STRING comment '国籍',--国籍
    UBO_RESIDENCE        STRING comment '居住地',--居住地
    RELATIONSHIP_TYPE    STRING comment '关系类型(DIRECT:直接,INDIRECT:间接)',--关系类型(DIRECT:直接,INDIRECT:间接)
    EFFECTIVE_DATE      DATE comment '生效日期',--生效日期
    EXPIRY_DATE         DATE comment '失效日期(NULL表示长期有效)',--失效日期(NULL表示长期有效)
    STATUS             STRING comment '状态(ACTIVE:有效,INACTIVE:失效)',--状态(ACTIVE:有效,INACTIVE:失效)
    CREATED_BY          STRING comment '创建人',--创建人
    CREATED_DATE        DATE comment '创建时间',--创建时间
    UPDATED_BY          STRING comment '更新人',--更新人
    UPDATED_DATE        DATE comment '更新日期'--更新日期
)PARTITIONED BY (ETL_DATE   STRING)
 STORED AS ORC
 LOCATION '/aml/ods/ODS_AML_UBO_INFO '
 TBLPROPERTIES ("orc.compress"="SNAPPY")
;

