INSERT OVERWRITE TABLE dwd.dim_aml_account PARTITION(etl_date)
SELECT
    200000 + row_number() over (order by account_id)  as account_sk,
    account_id,
    customer_id,
    acct_no,
    case
        when acct_type='SAVING' then '储蓄'
        when acct_type='CURRENT' then '活期'
        when acct_type='CREDIT' then '信用卡'
        when acct_type='LOAN' then '贷款'
        when acct_type='INVESTMENT' then '投资'
        else acct_type
        end as acct_type,
    currency  as currency_code,
    current_balance,
    avg_daily_balance,
    case
        when status='ACTIVE' then '活跃'
        when status='INACTIVE' then '休眠'
        when status='CLOSED' then '已销户'
        when status='FROZEN' then '冻结'
        else status
        end as status,
    branch_code,
    case
        when channel_open='COUNTER' then '柜面'
        when channel_open='ONLINE' then '网银'
        when channel_open='MOBILE' then '手机银行'
        else channel_open
        end as channel_open,
    open_date  as start_date,
    case
        when close_date is null then '9999-12-31'
        else close_date
        end  as end_date,
    etl_date
from ods.ods_aml_account_master
;