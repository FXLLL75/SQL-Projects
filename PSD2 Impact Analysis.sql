-- Create Table interval
DROP TABLE if exists sandbox.intervals_dsp2;
CREATE TABLE sandbox.intervals_dsp2 AS
SELECT inv.id,
   inv.start_date,
   inv.start_date::date as start_date_day,
   cer.base_currency_id,
   cer.avg_quotation_value
FROM dimensions.intervals AS inv
INNER JOIN consolidations.common_exchangerates cer ON (cer.interval_id = inv.id) -- conversion
AND ce.base_currency_id = tpp.devise_id 
AND ce.term_currency_id = '212' -- quoation value et term_currency euro

WHERE type = 'day'
AND inv.start_date >= '2021-05-01 00:00:00+00'::timestamptz
AND inv.start_date < '2021-09-01 00:00:00+00'::timestamptz;
ANALYZE sandbox.intervals_dsp2;
-- Create Index
CREATE INDEX ON sandbox.intervals_dsp2 (start_date);
CREATE INDEX ON sandbox.intervals_dsp2 (start_date_day);
CREATE INDEX ON sandbox.intervals_dsp2 (start_date_day, base_currency_id);


-------------------- RQT EXTRACT
DROP TABLE IF EXISTS sandbox.extract_dsp2_VF;
CREATE TABLE sandbox.extract_dsp2_VF
AS
SELECT
mer.business_id,
mer.company_name as merchant,
acq.label as acqueror,
psp.label as psp,
to_char(tpp.transaction_date, 'YYYY-MM') as trx_month,
to_char(tpp.transaction_date, 'YYYY-MM-DD') as trx_day,
eci.id as eci_id,
pcpm.label as payment_method,
tpp.issuing_institution as banque_emettrice,
tpp.acquirer_transaction_message as refus_acquereur,

Case when tpp.transaction_amount  >= '0.00' and tpp.transaction_amount < '31.00' then 'tranche 0-30' 
when tpp.transaction_amount  >= '31.00' and tpp.transaction_amount < '101.00' then 'tranche 31-100'    
when tpp.transaction_amount  >= '101.00' and tpp.transaction_amount < '251.00' then 'tranche 101-250' 
when tpp.transaction_amount  >= '251.00' and tpp.transaction_amount < '501.00' then 'tranche 251-500'  
when tpp.transaction_amount  >= '501.00' then 'tranche >500'end as tranches,

/*autre méthode pas retenue Case when tpp.transaction_amount::numeric  >= '0.00' and tpp.transaction_amount::numeric < '11.00' then 'tranche 1' 
when tpp.transaction_amount::numeric  >= '11.00' and tpp.transaction_amount::numeric < '21.00' then 'tranche 2'*/

--- check DSP2 (not needed) :
--tpp.authentication_indicator,
--tpp.authentication_status,

count(tpp.transaction_id) as created_trx,
count(case when st.authorized_and_pending is not null OR st.authorized is not null then tpp.transaction_id end)  as authed_trx,
count(case when st.authentication_failed is not null then tpp.transaction_id end) as authentication_fail_trx,
count(case when st.refused is not null then tpp.transaction_id end) as refused_transactions,
----by customer :
count(distinct CASE WHEN tpp.in_ecommerce_indicator_id in (1,2) THEN tpp.cardhashed ELSE tpp.customer_email END) as customers_created_trx,
count(distinct CASE WHEN (st.authorized is not null or st.authorized_and_pending is not null) THEN
					CASE
						WHEN tpp.in_ecommerce_indicator_id in (1,2) then tpp.cardhashed
						ELSE tpp.customer_email
					END END) AS authorized_customers_transactions

--(count(case when st.authorized_and_pending is not null OR st.authorized is not null then tpp.transaction_id end)) / (count(tpp.transaction_id)) as succes_rate,
--sum(case when st.authorized is not null or st.authorized_and_pending is not null THEN (tpp.transaction_amount * iD.avg_quotation_value) end) as authed_EUA
--(count(case when st.authorized_and_pending is not null OR st.authorized is not null then tpp.transaction_id end)) / (sum(case when st.authorized is not null or st.authorized_and_pending is not null THEN (tpp.transaction_amount * ce.avg_quotation_value) end)) as panier_moyen


FROM logs.payment_tpp_transaction tpp
INNER JOIN dimensions.common_countries cco ON cco.id = tpp.country_id AND cco.id = 76 -- focus payment_country FR
INNER JOIN dimensions.payment_applifi_merchants mer ON mer.id = tpp.merchant_id AND mer.business_id in ('1369542',
'1368152',
'1370925',
'1366016',
'1372605',
'1373680',
'1372674',
'1367398',
'1373100',
'1371404',
'1369424',
'1368336',
'1373526',
'1372940',
'1373307',
'1366102',
'1366698',
'1373129',
'1372928',
'1367859',
'1367419',
'1370478',
'1367357',
'1373429',
'1367599',
'13558717',
'1367180',
'1367362',
'1373273',
'1373038',
'1368267',
'1373519',
'1370772')
INNER JOIN dimensions.payment_common_ecommerce_indicator eci ON eci.id = tpp.in_ecommerce_indicator_id AND eci.id <> 8
INNER JOIN dimensions.payment_common_payment_method pcpm ON pcpm.id = tpp.payment_method_id
INNER JOIN scripts.payment_tpp_transaction_status st ON st.transaction_id = tpp.transaction_id
INNER JOIN dimensions.payment_common_acqueror acq ON acq.id = tpp.acqueror_id
INNER JOIN dimensions.payment_common_psp psp ON psp.id = tpp.psp_id
INNER JOIN sandbox.intervals_dsp2 iD ON iD.start_date_day = tpp.transaction_date::date AND iD.base_currency_id = tpp.devise_id 
-- jointure avec la log.payment_tpp_transaction sur la transaction_date

WHERE tpp.transaction_date >= '2021-05-01:00:00+00' AND tpp.transaction_date < '2021-09-01:00:00+00'

GROUP BY 1,2,3,4,5,6,7,8,9,10,11;

\copy (SELECT * FROM sandbox.extract_dsp2_VF) to ~/extract_dsp2_VF.csv csv header delimiter ';'
