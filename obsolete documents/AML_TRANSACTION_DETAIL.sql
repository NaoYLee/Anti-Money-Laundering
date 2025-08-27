CREATE TABLE  ODS.ODS_AML_TRANSACTION_DETAIL (
                                                 TXN_ID              bigint comment '交易唯一标识ID',
                                                 TXN_SEQ_NO          string comment '交易流水号（业务主键）',
                                                 ACCOUNT_ID          bigint comment '发起方账户ID（外键）',
                                                 TXN_TYPE            string comment '交易类型',
                                                 TXN_SUB_TYPE        string comment '交易子类型',
                                                 TXN_CHANNEL         string comment '交易渠道 ',
                                                 TXN_AMOUNT          bigint comment '交易金额',
                                                 CURRENCY            string comment '交易币种',
                                                 TXN_DATE            DATE comment ' 交易日期',
                                                 TXN_TIME            TIMESTAMP comment '交易时间戳',
                                                 TXN_STATUS          string comment '交易状态',
                                                 CORE_TXN_ID         string comment '核心系统原始交易ID',
                                                 REF_NO              string comment '参考号（如转账附言）',
                                                 REMITTER_NAME       string comment '汇款人姓名',
                                                 REMITTER_ACCT_NO    string comment '汇款人账号',
                                                 BENEFICIARY_NAME    string comment '收款人姓名',
                                                 BENEFICIARY_ACCT_NO string comment '收款人账号',
                                                 REMITTER_COUNTRY    string comment '汇款人国家（跨境）',
                                                 BENEFICIARY_COUNTRY string comment '收款人国家（跨境）',
                                                 IP_ADDRESS          string comment '交易发起IP地址（电子渠道）',
                                                 DEVICE_ID           string comment '设备ID（电子渠道）',
                                                 GEO_LOCATION        string comment '地理位置信息（电子渠道）',
                                                 CREATED_DATE        DATE comment '记录创建日期'
)COMMENT  '交易明细表'
    PARTITIONED BY (ETL_DATE   STRING)
    STORED AS ORC
    LOCATION '/aml/ods/ODS_AML_TRANSACTION_DETAIL'
    TBLPROPERTIES ("orc.compress"="SNAPPY") ;
