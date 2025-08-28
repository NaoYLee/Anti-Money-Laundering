CREATE TABLE IF NOT EXISTS dwd.dim_aml_date (
    date_sk               INT         COMMENT '代理键',
    full_date             DATE        COMMENT '日期',
    day_of_week           STRING      COMMENT '星期几',
    day_of_month          INT         COMMENT '月中第几天',
    week_of_year          INT         COMMENT '年中第几周',
    month_name            STRING      COMMENT '月份名称',
    quarter               INT         COMMENT '季度',
    year                  INT         COMMENT '年份',
    is_weekend            BOOLEAN     COMMENT '是否周末',
    fiscal_period         STRING      COMMENT '财年期间'
)
COMMENT '时间维度表'
STORED AS ORC;


INSERT OVERWRITE TABLE dwd.dim_aml_date
SELECT
    CAST(TO_DATE(full_date) AS INT) AS date_sk,
    full_date,
    CASE date_format(full_date, 'u')
        WHEN '1' THEN '星期一'
        WHEN '2' THEN '星期二'
        WHEN '3' THEN '星期三'
        WHEN '4' THEN '星期四'
        WHEN '5' THEN '星期五'
        WHEN '6' THEN '星期六'
        WHEN '7' THEN '星期日'
    END AS day_of_week,
    DAY(full_date) AS day_of_month,
    WEEKOFYEAR(full_date) AS week_of_year,
    CASE MONTH(full_date)
        WHEN 1 THEN '一月'
        WHEN 2 THEN '二月'
        WHEN 3 THEN '三月'
        WHEN 4 THEN '四月'
        WHEN 5 THEN '五月'
        WHEN 6 THEN '六月'
        WHEN 7 THEN '七月'
        WHEN 8 THEN '八月'
        WHEN 9 THEN '九月'
        WHEN 10 THEN '十月'
        WHEN 11 THEN '十一月'
        WHEN 12 THEN '十二月'
    END AS month_name,
    QUARTER(full_date) AS quarter,
    YEAR(full_date) AS year,
    date_format(full_date, 'u') IN ('6', '7') AS is_weekend,
    CONCAT('FY', YEAR(full_date), 'Q', QUARTER(full_date)) AS fiscal_period
FROM (
    SELECT date_add('2000-01-01', pos) AS full_date
    FROM (
        SELECT posexplode(split(space(10957), ' ')) AS (pos, dummy)
    ) t
) dates;