create or replace view CDP_DB.CDP_SCHEMA.CUSTOMER_TRANSACTION_SUMMARY(
	CUSTOMER_ID,
	BRAND,
	PROPRIETARY_BANK_TRANSACTION_CODE,
	MERCHANT_BRAND_CODE,
	MERCHANT_BRAND_NAME,
	MCC,
	MERCHANT_SYSTEM_CATEGORY1_NAME,
	MERCHANT_SYSTEM_CATEGORY2_NAME,
	MERCHANT_SYSTEM_CATEGORY3_NAME,
	FCA_CODE,
	FCA_CATEGORY,
	START_DATE,
	END_DATE,
	CREDIT_DEBIT_FLAG,
	TXN_COUNT,
	TXN_AMOUNT,
	DOB,
	OPEN_DATE,
	SEGMENT,
	POST_CODE_AREA
) as
select a.*,
DOB,
OPEN_DATE,
SEGMENT,
POST_CODE_AREA
from 
CDP_transaction_data a
inner join 
CDP_cust_data b
on (a.customer_id=b.customer_id);

create or replace view CDP_DB.CDP_SCHEMA.MONTHLY_CUSTOMER_BUDGET_MONITOR(
	START_DATE,
	CUSTOMER_ID,
	CATEGORY,
	AMOUNT_SPEND,
	BUDGET_AMOUNT,
	BUDGET_CUSTOMER_ID
) as
with 
customer_spend as
(
select 
start_date,
customer_id,
merchant_system_category1_name as category,
sum(txn_amount) as amount_spend
from CDP_transaction_data
where credit_debit_flag='D'
group by 
start_date,
customer_id,
merchant_system_category1_name
),
customer_budget as 
(
select * from CDP_budget_data
)
select a.*,budget_amount,b.customer_id as budget_customer_id from customer_spend a
left join customer_budget b
on (a.customer_id=b.customer_id     
    and category=budget_category
    );

create or replace view CDP_DB.CDP_SCHEMA.TOP_THREE_SQL(
	GENERATED_SQL,
	title,
	CHART_TYPE,
	X_AXIS,
	Y_AXIS,
	LEGEND_KEY,
	RANK_FLAG
) as
Select * from 
(
select 
    distinct 
    generated_sql,
	split_part(user_query,'__',2) as title,
    chart_type,
    x_axis,
    y_axis,
    legend_key,
    row_number() over(order by unique_id desc)  rank_flag
    from saved_queries 
where 
    user_action in ( 'Both','Save Analysis for Future Reference')
    and chart_preference='Yes'
) tmp 
where rank_flag <=3;