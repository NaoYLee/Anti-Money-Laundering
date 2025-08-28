#!/bin/bash

# Oracle数据库url
ORACLE_CONNECT="jdbc:oracle:thin:@//localhost:1521/orcl"
# Oracle数据库用户名
ORACLE_USER="aml"
# Oracle数据库密码
ORACLE_PASS="123456"
# Hive数据库名称
HIVE_DATABASE="AML_ODS"
# 日志文件路径
LOG_FILE="/var/log/sqoop_import.log"
# 需要导入的表列表
TABLES=(
    'AML_ACCOUNT_MASTER' 
    'AML_ALERT' 
    'AML_CUSTOMER_MASTER' 
    'AML_MONITORING_RULE' 
    'AML_SCREENING_RESULT' 
    'AML_SUSPICIOUS_TXN_REPORT' 
    'AML_TRANSACTION_DETAIL' 
    'AML_UBO_INFO' 
    'AML_WATCHLIST_DETAIL' 
    'AML_WATCHLIST_MASTER'
    )

# 初始化日志文件
rm -rf "$LOG_FILE"
mkdir -p "$(dirname "$LOG_FILE")"
touch "$LOG_FILE"
chmod 644 "$LOG_FILE"

# 记录导入开始时间
echo "======================================================================" >> "$LOG_FILE"
echo "导入开始时间: $(date +'%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"

# 导入单个表的函数
# 参数:
#   $1 - 要导入的表名
# 返回值:
#   Sqoop导入命令的返回状态码
table_import () {
    local table="$1"
    local etl_date

    echo "======================================================================" >> "$LOG_FILE"
    echo "开始导入表: $table" >> "$LOG_FILE"
    start_time=$(date +%s)
    etl_date="$(date +'%Y-%m-%d')"
    echo "开始时间：$start_time" >> "$LOG_FILE"

    # 执行Sqoop导入命令，将Oracle表数据导入到Hive表中
    sqoop import \
    --connect "$ORACLE_CONNECT" \
    --username "$ORACLE_USER" \
    --password "$ORACLE_PASS" \
    --table "$table" \
    --hcatalog-table 'ODS_'"$table" \
    --hcatalog-storage-stanza "stored as orc" \
    --hive-partition-key "etl_date" \
    --hive-partition-value "$etl_date" \
    --hcatalog-database "$HIVE_DATABASE" \
    -m 1 >> "$LOG_FILE" 2>&1

    return $?
}

# 标记所有表是否导入成功的变量
success=true

# 遍历所有表并执行导入操作
for table in "${TABLES[@]}"
do
    table_import "$table"

    status=$?
    lasting=$(( $(date +%s) - start_time ))

    if [ $status -eq 0 ]; then
        echo "表$table 导入成功" >> "$LOG_FILE"
        echo "执行时长：$lasting" >> "$LOG_FILE"
    else
        echo "表$table 导入失败" >> "$LOG_FILE"
        echo "执行时长：$lasting" >> "$LOG_FILE"
        success=false
    fi
done

# 根据导入结果输出最终日志并退出
if $success
then
    echo "所有表导入完毕" >> "$LOG_FILE"
    echo "======================================================================" >> "$LOG_FILE"
    exit 0
else
    echo "部分表导入失败" >> "$LOG_FILE"
    echo "======================================================================" >> "$LOG_FILE"
    exit 1
fi