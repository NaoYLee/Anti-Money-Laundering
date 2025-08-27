-- ODS层名单主表
CREATE TABLE IF NOT EXISTS ods.ods_aml_watchlist_master
(
    list_id        BIGINT COMMENT '名单唯一标识ID',
    list_code      STRING COMMENT '名单代码',
    list_name      STRING COMMENT '名单名称',
    list_source    STRING COMMENT '名单来源',
    list_type      STRING COMMENT '名单类型',
    currency       STRING COMMENT '适用币种 (NULL表示所有币种)',
    country        STRING COMMENT '适用国家 (NULL表示所有国家)',
    valid_from     STRING COMMENT '生效日期',
    valid_to       STRING COMMENT '失效日期 (NULL表示长期有效)',
    status         STRING COMMENT '名单状态',
    description    STRING COMMENT '名单描述',
    updated_date   STRING COMMENT '最后更新日期'
)
COMMENT '名单主表'
PARTITIONED BY (etl_date STRING COMMENT '数据加载日期')
STORED AS ORC
LOCATION '/data/aml/ods/watchlist_master'
TBLPROPERTIES ("orc.compress" = "SNAPPY");