/**                                                                          SAS DATA ANALYST
                                                                              Félix Goureau
                                                                              15/10/2021               
*/
/************************************************************************************************************************/
/* 			            STEP 1 : IMPORT .CSV DATAS INTO SQL TABLE		        			                */
/***********************************************************************************************************************/  
-----------------------------------------------------------------------------------------------------------------------
--------------------------- 1.1 CREATING THE SUBSCRIPTIONS_PERIODS TABLE FROM .CSV  :
-----------------------------------------------------------------------------------------------------------------------

-- Table: public.subscriptions_periods

-- DROP TABLE public.subscriptions_periods;

CREATE TABLE IF NOT EXISTS public.subscriptions_periods
(
    customer_id character varying(64) COLLATE pg_catalog."default" NOT NULL,
    subscription_id character varying(64) COLLATE pg_catalog."default" NOT NULL,
    plan_id character varying(30) COLLATE pg_catalog."default" NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    monthly_amount integer NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE public.subscriptions_periods
    OWNER to postgres;

---- We will chosee to replace out of range data : max(end_date) = '2021-10-15'
/*SUBSCRIPTIONS THAT END IN 2050 ARE CONSIDERED LIKE NO END DATE SUBSRIPTIONS.
WE CHOSE TO REPLACE THIS VALUE BY TODAY, IN ODRER TO MAKE THE DATA SET NOT TOO HEAVY.
AS A RESULT WE WILL STILL CONSIDER THAT THOSE SUBSCRIPTIONS AREN'T ENDED AND THE DATA SET IS LESS HEAVY*/

UPDATE subscriptions_periods
SET end_date = REPLACE(end_date, '2050-01-01', '2021-10-15') 
AS subscriptions_periods_filtred
;

-----------------------------------------------------------------------------------------------------------------------
--------------------------- 1.2 CREATING THE DAYS TABLE FROM .CSV  :
-----------------------------------------------------------------------------------------------------------------------

----------------------- Import days.csv days in tabe postgres :

CREATE TABLE IF NOT EXISTS public.days
(
    date_day date NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE public.days
    OWNER to postgres;

---- Same logic for the days table : max(date_day) = '2021-10-15'
UPDATE days
SET date_day = REPLACE(date_day, '2023-09-16', '2021-10-15') 
AS days_truncate
;

------------------------------------------------------------------------------
/* We have now our 2 tables, in present scope : 
i) subscriptions_periods_filtred
ii) days_truncate
-----------------------------------------------------------------------------------


/************************************************************************************************************************/
/*    STEP 2 : LEFT JOIN TABLE DAYS ON TABLE SUBSCRIPTIONS PERIOD TO HAVE TIME VARIABLE AND DAY BY DAY GRANULARITY DATA  */
/***********************************************************************************************************************/

/*Insert days rows from the utility table, in order to have date variable and not only start or end of subscription period date*/


DROP TABLE IF EXISTS work_days_sub_table ;
CREATE TABLE work_days_sub_table
AS
SELECT
s.customer_id,
s.subscription_id,
s.plan_id,
d.date_day as date_day,
to_char(d.date_day,'YYYY-MM') as date_month,
s.start_date,
s.end_date,
s.monthly_amount


from days d
 LEFT JOIN subscriptions_periods_filtred_2 s ON d.date_day >= s.start_date and d.date_day < s.end_date 
 ;
 -- insert date_day rows in subscriptions_periods, only if start_date =< day_date < end_date.

 SELECT
 count(*) FROM work_days_sub_table ;
 2 746 612 rows


 /************************************************************************************************************************/
/*                      STEP 3 : UPDATE TABLE WITH KEY METRICS WE WANT TO ANALYZE                         */
/***********************************************************************************************************************/

             /****************************************************/
/************ 3.1 CREATE NEW INDICATOR : DURATION OF THE SUBSCRIPTION  **********/
/***********************   CALCULATION TEST      **************************/
SELECT
to_char(date_day,'YYYY-MM') as date_month,
start_date,
end_date,
((DATE_PART('year',end_date)) - DATE_PART('year', start_date)) * 12 + (DATE_PART('month', end_date) - DATE_PART('month', start_date)) as sub_duration


from work_days_sub_table 

limit 200;
--- RESULT : CALCULATION IS OK



             /*************************************************/
/************ 3.2 UPDATE THE SUBSCRIPTIONS TABLE WITH OUR NEW INDICATOR ** /
 *********************************************************************/
DROP TABLE IF EXISTS work_days_sub_table_1 ;
CREATE TABLE work_days_sub_table_1
AS
SELECT
customer_id,
subscription_id,
plan_id,
date_day,
date_month,
start_date,
end_date,
monthly_amount,
((DATE_PART('year',end_date)) - DATE_PART('year', start_date)) * 12 + (DATE_PART('month', end_date) - DATE_PART('month', start_date)) as sub_duration



from work_days_sub_table ;



               /*********************************************************/
--/************ 3.3 UPDATE AGAIN THE TABLE WITH SUB_REVENUES CALCULATION ******/
/***********    WE WANT IT DIRECTLY AS A COLUMN IN OUR TABLE        ********* /
 /*********************************************************************/ 
 
DROP TABLE IF EXISTS work_days_sub_table_2 ;
CREATE TABLE work_days_sub_table_2
AS
SELECT
customer_id,
subscription_id,
plan_id,
date_day,
date_month,
start_date,
end_date,
sub_duration,
monthly_amount,
(sub_duration * monthly_amount) as sub_revenues


from work_days_sub_table_1 ;


/****************************************************************************************************************************************/
                                     /*DATA QUALITY - EXTRA MINOR SLOW DOWNS                                                     */
/****************************************************************************************************************************************/

/*Customer_id and subscriptions_id are not unique. count(distinct) <> count. from subscriptions table or from left joined with days table
Data date format had to be changed to import in sql engine as date.
monthly amount = - replaced by 0 to import in a sql engine.
We can also note one subscription for which the start date >  end date. (fixed after left joind with days)

SUBSCRIPTIONS THAT END IN 2050 ARE CONSIDERED LIKE NO END DATE SUBSRIPTIONS.
WE CHOSE TO REPLACE THIS VALUE BY TODAY, IN ODRER TO MAKE THE DATA SET NOT TOO HEAVY.
WE STILL CONSIDER THAT THOSE SUBSCRIPTIONS AREN'T ENDED.
*/





/************************************************************************************************************************/
                                            /*STEP 4 : DATA ANALYSIS VIA SQL ENGINE                                  */
/***********************************************************************************************************************/ 

---------------------------------------------- 4.1 ANALYZING DATA BASE : ---------------------------------------------------
--------------------------------------------------------------------------------------------------


---- SUBSCRIPTIONS DATE RANGE ANALYSIS :
SELECT
min(start_date) as min_start_date,
max(end_date) as max_end_date

FROM work_days_sub_table_2
;

--- results :
"2017-06-29" "2020-10-15"
---- days date_day range :
SELECT
min(date_day),
max(date_day)

from days_truncate ;
-- resulsts :
"2017-01-01"    "2021-10-15"
--- We will be able to calculate metrics with day granluraty only in this scope.


----- Check end_date & start_date coherence :
SELECT
customer_id
FROM subscriptions_periods_filtred
WHERE (start_date > end_date) ; 

---- results :
"bccfd2339b2993520032c4cc42c489df8a4ccf862e9f77ad11bf9c4b89850903"


---- Fixed when days are inserted :
SELECT
count(customer_id)
FROM work_days_sub_table
WHERE (start_date > end_date) ;
-- results :
0


-----------------------------------------------------------------------------------------------------------------------
--------------------------- 4.2 HAVE A LOOK AT MIN MAX AVG VALUES OF DATA SET :
-----------------------------------------------------------------------------------------------------------------------



------------------------------------------- Average and max subscriptions duration and revenues Overall

SELECT
avg(sub_duration),
max(sub_duration),
avg(sub_revenues),
max(sub_revenues)

from work_days_sub_table_2 ;

|avg_sub_duration | max_sub_duration | average_revenues  | max_sub_revenues |
 23.56721668392982       52            7 866.348206234257     1 529 187

------------------ Average monthly amount overall is 304€ :

SELECT
min(monthly_amount),
avg(monthly_amount),
max(monthly_amount)

from work_days_sub_table_2 ;

min | average             | max
0     304.2367995128439338  46 339







/************************************************************************************************************************/
/*                                    STEP 5 : ASSUMPTIONS ON BUSINESS RULES :                                            */
/***********************************************************************************************************************/



-------------------- H1 : Customers have more than one subscriptions ?
DROP TABLE IF EXISTS quanti_client_base_analysis ;
CREATE TABLE quanti_client_base_analysis
AS
SELECT
count(distinct customer_id) as unique_customer,
count(distinct subscription_id) as unique_sub,
count(distinct plan_id) as unique_plan

FROM work_days_sub_table_2
;
----

SELECT * FROM quanti_client_base_analysis ;

--- results :
 unique_customer | unique_sub |  unique_plan |
 4 434              4 695           29
 -- Average subscriptions per customers :
SELECT
(unique_sub / unique_customer) as avg_number_of_subscriptions_by_customer    

FROM quanti_client_base_analysis ;

--- results :
avg_number_of_subscriptions_by_customer
        1.0588633288227334

-------------------- H2 : Can we identify some offers that are more attractive for customers ?        

----------------- Repartition of customers by offers :
--------- DISTINCT CUSTOMER REPARTITION BY OFFERS :             
SELECT
plan_id,
count(distinct customer_id)

FROM days_sub_table_significant_plans

GROUP BY 1 ;



---- 95% OF CUSTOMERS ARE DISTRIBUTED IN 12 OFFERS :
'api_advanced_12'
'api_basic_12'
'api_extended_12'
'app_corpo'
'app_corpo_12'
'app_enterprise'
'app_enterprise_12'
'app_enterprise_24'
'app_notary'
'app_pro'
'app_pro_12'
'app_pro_24'

--- CREATE TABLE WITH SIGNIFICATIVE PLANS (95% of the customers) :

CREATE TABLE days_sub_table_significant_plans AS
SELECT *
FROM work_days_sub_table_2
WHERE plan_id in ('api_advanced_12',
                  'api_basic_12',
                  'api_extended_12',
                  'app_corpo',
                  'app_corpo_12',
                  'app_enterprise',
                  'app_enterprise_12',
                  'app_enterprise_24',
                  'app_notary',
                  'app_pro',
                  'app_pro_12',
                  'app_pro_24')
-----------------------------------------------------------------------------------






-------------------- H3: Is there a link between offers and their duration ?
--

SELECT
distinct plan_id,
sub_duration


from days_sub_table_significant_plans
;
-- returns 548 rows : There is 548 plans with unique duration of subscription. 
-- Considering there is only 29 different plans, we can assume that there is no correlation betweend the type of offers and the duration of the subscription.

---- average sub_duration of plans :
SELECT
plan_id,
avg(sub_duration)

from days_sub_table_significant_plans
group by 1
;


------------------- average duration of SAS offers is 2 years

SELECT
avg(sub_duration),
max(sub_duration)

from work_days_sub_table_2 ;
min | average         | max
0    23.56721668392982   52


-------------------- H4 : Do monthly subscriptions amounts depend on plans ?
--
SELECT
distinct plan_id,
monthly_amount


from days_sub_table_significant_plans
;
-- returns 419 rows : There is 419 different monthly subscriptions fees by plans


--- overall average :
SELECT
avg(monthly_amount)
FROM work_days_sub_table_2 ;
----- results :
304.2367995128439338

----- extract :
SELECT
plan_id,
avg(monthly_amount)
FROM work_days_sub_table_2 
GROUP BY 1 ;


--------------------------------------
SELECT
date_month,
plan_id,
sub_revenues,
count(distinct customer_id)

FROM work_days_sub_table_2

where date_month >= '2020-01-01' AND date_month < '2021-01-01' 
GROUP BY 1,2,3
;




----- H5 : Some Monthly Fees depend on subscriptions ?
--- Extract :
SELECT
distinct plan_id,
sub_duration,
monthly_amount

FROM days_sub_table_significant_plans

WHERE plan_id in ('app_enterprise','app_enterprise_12', 'app_enterprise_24', 'app_pro', 'app_pro_12', 'app_pro_24') ;


----- H6 : Can we identify offers that generated more revenues for SAS ?


---- sub revenue over the period :
with revenues
AS(
SELECT
distinct(plan_id),
sub_revenues

FROM work_days_sub_table_2 )

SELECT
sum(sub_revenues)
FROM revenues
;
--- Results :
18 203 812 €
--- avg sub revenues
SELECT
avg(sub_revenues),

from work_days_sub_table_2 ;
--- Results :
7 866.348206234257

-- Extract :
with revenues
AS(
SELECT
distinct(plan_id),
sub_revenues

FROM work_days_sub_table_2 )

SELECT
distinct plan_id,
sum(sub_revenues)
FROM revenues
GROUP BY 1
;


/************************************************************************************************************************/
/*                       STEP 6 : CHURN CUSTOMERS AND CHURN AMOUNT */
/***********************************************************************************************************************/ 


-----------------------------------------------------------------------------------------------------------------------
--------------------------- 6.1 NUMBER OF CUSTOMERS THAT CHURNED DURING JUNE 2020 :
-----------------------------------------------------------------------------------------------------------------------


-------------- 
---- defining churn analysis scope :
WITH active_clients_on_scope
AS 
(SELECT *
FROM work_days_sub_table_2 
WHERE start_date < '2020-06-01' AND end_date > '2020-06-01' AND date_month = '2020-06'),
---- when the period starts, customer is subscribed.
--- subscriptions has started before 1st june and didn't ended before 1st june, date month is june 2020
-- => customer active at the start of the month

--- identify customers that are active at the start of the mobth and those that cancelled their subscriptions on the period:
customer_status
AS 
(SELECT 
CASE
  WHEN (end_date > '2020-06-30') -- if he cancelled out of scope (june 2020), he is not counted cancelled
    THEN 0
  ELSE 1
  END as is_canceled, 
CASE
  WHEN (start_date < '2020-06-01'
    AND 
      end_date >= '2020-06-01'
      
    ) THEN 1
    ELSE 0
  END as is_active
FROM active_clients_on_scope)

SELECT
SUM(is_canceled) as churned_customers,
SUM(is_active) as active_customers,
1.0 * SUM(is_canceled)/SUM(is_active) as churn_rate
FROM customer_status ;
-- Results :
churned_customers | active_customers | churn_rate
 3 392               41 460           0.0818137964302942

    



-----------------------------------------------------------------------------------------------------------------------
--------------------------- 6.2 REVENUE LOSS EVOLUTION OVER 2020 (CHURNED AMOUNT) :
-----------------------------------------------------------------------------------------------------------------------

------------- REVENUE LOSS OVER 2020 = AVERAGE REVENUE BY CUSTOMER * NB CUSTOMER LOST, EACH MONTH OVER 2020.

-- AVG SUB MONTHLY AMOUNT, EACH MONTH, OVER 2020 :

SELECT
date_month,
avg(monthly_amount)

FROM work_days_sub_table_2

WHERE date_month >= '2020-01' AND date_month < '2021-01-01'
GROUP BY 1
;
"2020-01" 321.3445636101596571
"2020-02" 325.9742001109672647
"2020-03" 326.6067963492799258
"2020-04" 322.3677069199457259
"2020-05" 309.4252674655260609
"2020-06" 302.9403865882520238
"2020-07" 295.5128216720617755
"2020-08" 294.0305942766930076
"2020-09" 289.4809633937971385
"2020-10" 288.4901113814276021
"2020-11" 288.4660114449947394
"2020-12" 288.4134404372489752



-----------------------------------------------------------------------------------------------------------------------
--------------------------- 6.3 CREATE TABLE FOR CHURNED CUSTOMERS OVER 2020 :
-----------------------------------------------------------------------------------------------------------------------
-- 
DROP TABLE IF EXISTS active_clients_on_scope_2020 ;
CREATE TABLE active_clients_on_scope_2020
AS 
SELECT *
FROM work_days_sub_table_2 
WHERE start_date < '2020-01-01' AND end_date > '2020-01-01' AND date_month >= '2020-01' AND date_month <'2021-01';

DROP TABLE IF EXISTS customer_status_2020;
CREATE TABLE customer_status_2020
AS 
SELECT
 date_month,
 monthly_amount,
CASE
  WHEN (end_date > '2020-12-31') -- if he cancelled out of scope (2020), he is not counted cancelled
    THEN 0
  ELSE 1
  END as is_canceled, 
CASE
  WHEN (start_date < '2020-01-01'
    AND 
      end_date >= '2020-12-31'
      
    ) THEN 1
    ELSE 0
  END as is_active
FROM active_clients_on_scope_2020
;
-------- results :
SELECT
date_month,
SUM(is_canceled) as churned_customers,
SUM(is_active) as active_customers,
1.0 * SUM(is_canceled)/SUM(is_active) as churn_rate
FROM customer_status_2020 
GROUP BY 1
;

----------------------------------- CHURNED AMOUNT = NB Churned customers * AVG(Monthly Amount)

SELECT
date_month,
(SUM(is_canceled) * avg(monthly_amount))

FROM customer_status_2020

WHERE date_month >= '2020-01' AND date_month < '2021-01-01'
GROUP BY 1
;
---- Results: 
"2020-01" 2597400.7552337787196196
"2020-02" 2188877.8216900386327764
"2020-03" 2149186.3181082939061164
"2020-04" 1889919.9098164574380120
"2020-05" 1612780.4474456225640507
"2020-06" 1127091.2505466065771392
"2020-07" 725971.3744134848450246
"2020-08" 334885.5835007763266230
"2020-09" 91241.2216519570222666
"2020-10" 45353.2210604755140540
"2020-11" 29890.6954259700845475
"2020-12" 14082.9240175487725211


/*************************************************************************************************************/
----ALTERNATIVE LONGER METHOD FOR CHURN RATE :
with monthly_usage as (
  select 
    customer_id, 
    date_month = '2020-01' as time_period
  from work_days_sub_table_2
  ),
 ----------- For each customer_id row , what is the next and previous month they subscribed, partitioned by user.
lag_lead as (
  select customer_id, time_period,
    lag(time_period,1) over (partition by customer_id order by customer_id, time_period),
    lead(time_period,1) over (partition by customer_id order by customer_id, time_period)
  from monthly_usage),
 /*what's the difference between our current month and the next month.
  If 1 = customer came back the next month. 
  If >= 2 then churn*/
lag_lead_with_diffs as (
  select customer_id, time_period, lag, lead, 
    time_period-lag lag_size, 
    lead-time_period lead_size 
  from lag_lead),
 
calculated as (select time_period,
  case when lag is null then 'NEW'
     when lag_size = 1 then 'ACTIVE'
     when lag_size > 1 then 'RETURN'
  end as this_month_value,
 
  case when (lead_size > 1 OR lead_size IS NULL) then 'CHURN'
     else NULL
  end as next_month_churn,
 
  count(distinct customer_id)
   from lag_lead_with_diffs
  group by 1,2,3)
 
select time_period, this_month_value, sum(count) 
  from calculated group by 1,2
union
select time_period+1, 'CHURN', count 
  from calculated where next_month_churn is not null
order by 1
-------------------------------------------------------------------------------------------------------------------------------------------------------------





























