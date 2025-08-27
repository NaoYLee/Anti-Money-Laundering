CREATE EXTERNAL TABLE if not exists ODS.ODS_AML_WATCHLIST_DETAIL (
    ENTITY_ID           BIGINT comment '名单条目唯一标识ID',--名单条目唯一标识ID
    LIST_ID             BIGINT comment '关联名单ID',--关联名单ID
    ENTITY_TYPE         STRING comment '实体类型(PERSON:个人,ORG:组织)',--实体类型(PERSON:个人,ORG:组织)
    NAME                STRING comment '实体名称',--实体名称
    ALIAS_NAME          STRING comment '别名/曾用名',--别名/曾用名
    ID_TYPE             STRING comment '证件类型',--证件类型
    ID_NO               STRING comment '证件号码',--证件号码
    NATIONALITY         STRING comment '国籍',--国籍
    BIRTH_DATE          DATE comment '出生日期(个人)',--出生日期(个人)
    PLACE_OF_BIRTH      STRING comment '出生地(个人)',--出生地(个人)
    ADDRESS             STRING comment '地址',-- 地址
    LISTED_DATE         DATE comment '列入日期',--列入日期
    DELISTED_DATE       DATE comment '移除日期(NULL表示仍在名单)',--移除日期(NULL表示仍在名单)
    REASON              STRING comment '列入原因',--列入原因
    SANCTION_TYPE       STRING comment '制裁类型',--制裁类型
    REFERENCE_INFO      STRING comment '参考信息(如决议号)',--参考信息(如决议号)
    STATUS              STRING comment '条目状态',--条目状态
    CREATED_DATE        DATE comment '创建日期',--创建日期
    UPDATED_DATE        DATE comment '更新日期'--更新日期
)PARTITIONED BY (ETL_DATE   STRING)
 STORED AS ORC
 LOCATION '/aml/ods/ODS_AML_WATCHLIST_DETAIL '
 TBLPROPERTIES ("orc.compress"="SNAPPY");