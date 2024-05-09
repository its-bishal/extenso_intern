create database client_rw;

use client_rw;

select count(*) from fc_account_master;

select count(*) from fc_transaction_base;


select * from fc_account_master LIMIT 10;

select * from fc_transaction_base LIMIT 10;

select count(*) from fc_transaction_base where is_salary is NULL;

alter table fc_transaction_base
rename column ï»¿tran_date to tran_date;


-- average monthly deposit 
select avg(lcy_amount) as avg_monthly_deposit, month(tran_date) from fc_transaction_base
where dc_indicator = 'deposit'
group by month(tran_date);

-- average monthly withdraw
select avg(lcy_amount) as avg_monthly_withdrawal from fc_transaction_base
where dc_indicator = 'withdraw'
group by tran_date; 


select
case when dc_indicator = 'deposit' then 'deposit' 
	 when dc_indicator = 'withdraw' then 'withdraw'
end as trans_cat,
avg(lcy_amount) as avg_amount
from fc_transaction_base
group by trans_cat;




-- single implementation
create table fc_deposit_facts as select * from(
select
fc_transaction_base.account_number as acc_num,
fc_account_master.customer_code as cust_code,
month(fc_transaction_base.tran_date),
avg(case when fc_transaction_base.dc_indicator = 'deposit' then fc_transaction_base.lcy_amount else null end) as avg_monthly_deposit,
std(case when fc_transaction_base.dc_indicator = 'deposit' then fc_transaction_base.lcy_amount else null end) as std_montly_deposit,
variance(case when fc_transaction_base.dc_indicator = 'deposit' then fc_transaction_base.lcy_amount else null end) as var_monthly_deposit,
avg(case when fc_transaction_base.dc_indicator = 'withdraw' then fc_transaction_base.lcy_amount else null end) as avg_monthly_withdraw,
std(case when fc_transaction_base.dc_indicator = 'withdraw' then fc_transaction_base.lcy_amount else null end) as std_montly_withdraw,
variance(case when fc_transaction_base.dc_indicator = 'withdraw' then fc_transaction_base.lcy_amount else null end) as var_monthly_withdraw
from fc_transaction_base join fc_account_master on fc_account_master.account_number = fc_transaction_base.account_number
group by fc_transaction_base.account_number,fc_account_master.customer_code, month(fc_transaction_base.tran_date)) as bishal;


create database fc_facts;
use fc_facts;
select* from fc_deposit_facts;