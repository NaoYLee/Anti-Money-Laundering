#!/bin/bash

ORACLE_CONNECT="jdbc:oracle:thin:@tcp://localhost:1521:orcl"
ORACLE_USER="aml"
ORACLE_PASS="123456"
HDFS_TARGET_DIR="/data/aml/ods"
LOG_FILE="/var/log/sqoop_import.log"
TABLES=(
    "AML_ACCOUNT_MASTER" 
    "AML_ALERT" 
    "AML_CUSTOMER_MASTER" 
    "AML_MONITORING_RULE" 
    "AML_SCREENING_RESULT" 
    "AML_SUSPICIOUS_TXN_REPORT" 
    "AML_TRANSACTION_DETAIL" 
    "AML_UBO_INFO" 
    "AML_WATCHLIST_DETAIL" 
    "AML_WATCHLIST_MASTER"
    )

rm -rf "$LOG_FILE"
mkdir -p "$(dirname "$LOG_FILE")"
touch "$LOG_FILE"
chmod 644 "$LOG_FILE"

echo "======================================================================" >> "$LOG_FILE"
echo "导出开始时间: $(date +'%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"

table_import () {
    local table="$1"
    local hdfs_dir="${HDFS_TARGET_DIR}/${table}"

    echo "开始导入表: $table" >> "$LOG_FILE"
    start_time=$(date +%s)
    echo "开始时间：$start_time" >> "$LOG_FILE"

    sqoop import \
    --connect "$ORACLE_CONNECT" \
    --username "$ORACLE_USER" \
    --password "$ORACLE_PASS" \
    --table "$table" \
    --target-dir "$hdfs_dir" \
    --fields-terminated-by ',' \
    --as-orcfile \
    -m 1 >> "$LOG_FILE" 2>&1

    return $?
}

success=true

for table in "${TABLES[@]}"
do
    hdfs dfs -test -d "${HDFS_TARGET_DIR}/${table}"
    return_code=$?
    if [ $return_code -eq 0 ]
    then
        hdfs dfs -rm -r -f "${HDFS_TARGET_DIR}/${table}" >> "$LOG_FILE"
    fi

    table_import "$table"

    status=$?
    lasting=$(( $(date +%s) - start_time ))

    if [ $status -eq 0 ]; then
        echo "表$table 导出成功" >> "$LOG_FILE"
        echo "执行时长：$lasting" >> "$LOG_FILE"
    else
        echo "表$table 导出失败" >> "$LOG_FILE"
        echo "执行时长：$lasting" >> "$LOG_FILE"
        success=false
    fi
done

if $success
then
    echo "所有表导出完毕" >> "$LOG_FILE"
    echo "======================================================================" >> "$LOG_FILE"
    exit 0
else
    echo "部分表导出失败" >> "$LOG_FILE"
    echo "======================================================================" >> "$LOG_FILE"
    exit 1
fi