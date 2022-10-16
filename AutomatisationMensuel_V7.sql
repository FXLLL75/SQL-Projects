/**************************************************************************************************************************/ 
/* 											Automatisation du Reporting Mensuel 		       				      */
/*											            V.3						     			  */
/**************************************************************************************************************************/

/*

-- Spécifications techniques :  https://docs.google.com/document/d/1JPRqr8alrxJJH7Sro1jpl25XcTmQVa9x/edit
-- Tableau de référence : https://drive.google.com/drive/u/0/folders/1wGnm5oeqKfQHBP_jRaqNhYNyjtRd7z-W
-- Présentation type : https://drive.google.com/drive/u/0/folders/1UobeqN3mqOfiJDJWykgzuyOz69e7EcwJ

--V2.1 : Rajout de la dimension pour le flag top DM
         Correction du filtre par type de transaction sur wallet (calcul du flux transactionnel)

-- V3 : Création d'un source unique pour une alimentation automatique en fin de mois 
		- Rajout la dimension marchand pour l'analyse financière 
		- Vérifier si les champs suivants sont présents dans l'analyse journalière : marchands, trx_day, flux end user capturé et vol trx capturé.  
		- Comment industrialiser le fichier DM 
		- Faire l'étude pour la création d'une catégorisation des marchands et comment l'industrialiser 
		- Le fichier DM s'execute avant dans un script à part  

Etude à faire : Catégorisation marchand + automatisation de la catégorisation de longtail 
Découper le retail en des granularité plus fines : Automobile, Fashion, etc..

(Il faut créer une jointure entre la BDD finance et la BDD avec la dim marchand dans tableau) ?? 

-- Règles pour l'historique des dates 
   Analyse mensuel : données de l'année en cours et N-1 à partir de janvier 
   Analyse hebdomadaire : Les 8 dernières semaines 
   Analyse journalière : Mois en cours + M-1 

--V5
remplacement de la table de DM : schéma sandbox => schéma metadata
*/


--envoyer fichier
scp /home/fgoureau/_liste_dm.csv fgoureau@dwh-etx.reporting.db:~

--scp /home/dfall/dl/liste_dm.csv df@dwh.reporting.dev:~
/*******************************************************************************************************
Truncate Table

Historique pour la création :

CREATE TABLE metadata.extract_fichier_dm_cog(
business_id integer,
sub_account_name text,
account_id integer PRIMARY KEY,
company_name text,
insert_date timestamptz default now()
);

ALTER TABLE metadata.extract_fichier_dm_cog OWNER TO dw;
*******************************************************************************************************/

TRUNCATE  metadata.extract_fichier_dm_cog;

--copie des lignes
COPY metadata.extract_fichier_dm_cog(
				 business_id,
                 sub_account_name,
                 account_id,
                 company_name
)

FROM '/home/fgoureau/liste_dm.csv' DELIMITER ';' CSV HEADER;


DROP TABLE IF EXISTS reporting_payment.truncate_insert_reporting_monthly_transaction; 
CREATE TABLE reporting_payment.truncate_insert_reporting_monthly_transaction (
	         product text,
	         transaction_month text,
	         transaction_week text,
	         transaction_day text,
             company_name text, 
             business_id integer, 
             sub_account_name text, 
             account_id integer,
             is_merchant_dm integer, 
             activity_sector text,
             authentication_indicator integer, 
             payment_method text,
             payment_country text, 
             currency text, 
             psp text, 
             acquirer text,
             branch text,
             ecommerce_indicator text, 
             trx_volume_created integer, 
             trx_volume_authorized integer,
             trx_volume_captured integer, 
             end_user_amount_created numeric, 
             end_user_amount_authorized numeric, 
             end_user_amount_captured numeric
 
);
ALTER TABLE reporting_payment.truncate_insert_reporting_monthly_transaction OWNER TO dw;

DROP TABLE IF EXISTS reporting_payment.truncate_insert_reporting_monthly_financial;
CREATE TABLE reporting_payment.truncate_insert_reporting_monthly_financial (
			product text, 
			financial_month text,
			company_name text, 
            business_id integer, 
            sub_account_name text, 
            account_id integer,
            is_transaction_fee integer, 
            operation_type text,
            payment_product text,
          --payment_method_fi text,
		    turnover numeric, 
			payout numeric, 
			margin numeric
);
ALTER TABLE reporting_payment.truncate_insert_reporting_monthly_financial OWNER TO dw;



drop view reporting_payment.v_monthly_reporting_transaction_financial;
drop view reporting_payment.v_transaction_financial_monthly_reporting;


/************************************************************************************************************************/
/* 			             ETAPE 1 : CREATION DE LA FUNCTION POUR L'ALIMENTATION         			                        */
/***********************************************************************************************************************/ 

-----------------------------------------------------------------------------------------------------------------------
--------------------------- 1.1 Création de function 
-----------------------------------------------------------------------------------------------------------------------


	CREATE OR REPLACE FUNCTION reporting_payment.truncate_insert_reporting_monthly (
	     )
		RETURNS void
		LANGUAGE 'plpgsql'

		COST 100 
		VOLATILE 

	AS $BODY$

	-----------------------------------------------------------------------------------------------------------------------
	--------------------------- 1.2 Déclaration des paramétres 
	-----------------------------------------------------------------------------------------------------------------------
	DECLARE 

	--intervales
	v_start TIMESTAMPTZ; 
	v_end TIMESTAMPTZ;

	--queries
	v_start_month DATE;
	v_end_month DATE;
	v_start_week DATE;
	v_end_week DATE;
	v_start_day DATE;
	v_end_day DATE;

	-----------------------------------------------------------------------------------------------------------------------
	--------------------------- 1.3 Début de la fonction
	-----------------------------------------------------------------------------------------------------------------------

	BEGIN

	RAISE LOG '[%] REPORTING_MONTHLY BEGIN FONCTION', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');

	--workmem
	set work_mem='5GB';

	--instanciation variables
	v_start := (date_trunc ('year', now() - interval '2 year'))::timestamptz;
	--v_start := (date_trunc ('month', now ()) - interval '2 months'))::timestamptz;
	v_end := (date_trunc('month', now()))::timestamptz;


	--Granularities
	-- Y-1 to M-1 
	v_start_month := (date_trunc ('year', now()) - interval '2 year')::date;
	v_end_month := (date_trunc('month', now()))::date;

	--W of (M-2) TO W-1
	v_start_week := (date_trunc('week', date_trunc('month', now()) - interval '2 months'))::date;
	v_end_week := (date_trunc('week', now()))::date;

	--M-1
	v_start_day := (date_trunc('month', now()) - interval '2 months')::date;
	v_end_day := (date_trunc('month', now()))::date;


	-- Truncate final table 
	TRUNCATE TABLE reporting_payment.truncate_insert_reporting_monthly_transaction;

	/************************************************************************************************************************/
	/* 			             ETAPE 2 : CREATION DE TABLES TEMPORAIRES POUR ALIMENTATION        			                    */
	/***********************************************************************************************************************/ 

	-----------------------------------------------------------------------------------------------------------------------
	--------------------------- 2.1 Création de la table temporaire interval 
	-----------------------------------------------------------------------------------------------------------------------

	RAISE LOG '[%] REPORTING_MONTHLY CREATE TEMP TABLE INTERVAL', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');

	DROP TABLE IF EXISTS t_intervals_reporting_mensuel;
	CREATE TEMP TABLE t_intervals_reporting_mensuel ON COMMIT DROP AS 
	    SELECT
		   inv.id,
		   inv.start_date::date as start_date_day,
		   cer.base_currency_id,
		   cer.avg_quotation_value
		FROM dimensions.intervals AS inv
		INNER JOIN consolidations.common_exchangerates cer ON (cer.interval_id = inv.id)
		WHERE inv.type = 'day' 
		AND inv.start_date >= v_start
		AND inv.start_date < v_end;

	CREATE INDEX ON t_intervals_reporting_mensuel (start_date_day, base_currency_id);

	-----------------------------------------------------------------------------------------------------------------------
	--------------------------- 2.2 Création de la table temporaire calcul marchand dm  
	-----------------------------------------------------------------------------------------------------------------------

	RAISE LOG '[%] REPORTING_MONTHLY CREATE TEMP TABLE DM MERCHANT', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');

	DROP TABLE IF EXISTS t_top_marchands_DM;
	CREATE TEMP TABLE t_top_marchands_DM ON COMMIT DROP AS 
	SELECT
		mer.id AS merchant_id, 
		dm.business_id,
		dm.sub_account_name, 
		dm.account_id, 
		dm.company_name 
	FROM dimensions.payment_applifi_merchants mer
	INNER JOIN metadata.extract_fichier_dm_cog dm ON (dm.account_id = mer.account_id AND mer.origin_entity = 'tpp');
	CREATE INDEX ON t_top_marchands_DM (merchant_id);

	/************************************************************************************************************************/
	/* 			             ETAPE 3 : INSERTION DES DONNEES TRANSACTIONNELLES       			                    */
	/***********************************************************************************************************************/ 

	-----------------------------------------------------------------------------------------------------------------------
	--------------------------- 3.1 Insertion des données transactionnelles TPP  
	-----------------------------------------------------------------------------------------------------------------------
	RAISE LOG '[%] REPORTING_MONTHLY INSERT TRANSACTION TPP 1', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');

	DROP TABLE IF EXISTS t_tpp_transaction;
	CREATE TEMP TABLE t_tpp_transaction ON COMMIT DROP AS
	SELECT
		tpp.transaction_id,
		tpp.transaction_date::date AS transaction_date,
		tpp.transaction_date AS transaction_date_timstamptz,
		tpp.authentication_indicator,
		tpp.transaction_amount,
		tpp.devise_id,
		tpp.merchant_id,
		tpp.country_id,
		tpp.psp_id,
		tpp.acqueror_id,
		tpp.in_ecommerce_indicator_id,
		tpp.payment_method_id,
	    mer.company_name, 
	   	mer.business_id, 
	   	mer.sub_account_name, 
	   	mer.account_id,
	   	mer.activity_sector_id,
	   	i.avg_quotation_value
	FROM logs.payment_tpp_transaction tpp
	INNER JOIN dimensions.payment_applifi_merchants mer ON mer.id = tpp.merchant_id AND mer.business_id <> '134655'
	INNER JOIN t_intervals_reporting_mensuel i ON (i.start_date_day = tpp.transaction_date::date AND i.base_currency_id = tpp.devise_id) 
	WHERE
		tpp.transaction_date >= v_start AND
		tpp.transaction_date < v_end AND
		tpp.origin_product = 'Enterprise';

	CREATE INDEX ON t_tpp_transaction(transaction_id);
	CREATE INDEX ON t_tpp_transaction(transaction_date);
	CREATE INDEX ON t_tpp_transaction(devise_id);
	CREATE INDEX ON t_tpp_transaction(merchant_id);
	CREATE INDEX ON t_tpp_transaction(country_id);
	CREATE INDEX ON t_tpp_transaction(psp_id);
	CREATE INDEX ON t_tpp_transaction(acqueror_id);
	CREATE INDEX ON t_tpp_transaction(in_ecommerce_indicator_id);
	CREATE INDEX ON t_tpp_transaction(payment_method_id);
	CREATE INDEX ON t_tpp_transaction(activity_sector_id);

	RAISE LOG '[%] REPORTING_MONTHLY INSERT TRANSACTION TPP 2', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');

	INSERT INTO reporting_payment.truncate_insert_reporting_monthly_transaction

	SELECT

		'Enterprise' AS product,

		CASE WHEN tpp.transaction_date >= v_start_month AND tpp.transaction_date < v_end_month
		THEN TO_CHAR(tpp.transaction_date_timstamptz AT TIME ZONE 'Europe/Paris', 'YYYY-MM'::text) 
		ELSE NULL::TEXT
		END AS transaction_month,

		CASE WHEN tpp.transaction_date >= v_start_week  AND tpp.transaction_date < v_end_week
		THEN TO_CHAR(date_trunc('week', tpp.transaction_date_timstamptz AT TIME ZONE 'Europe/Paris'), 'YYYY-MM-DD'::text) 
		ELSE NULL::TEXT
		END AS transaction_week,

		CASE WHEN tpp.transaction_date >= v_start_day AND tpp.transaction_date < v_end_day
		THEN to_char(tpp.transaction_date_timstamptz AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD'::text)
		ELSE NULL::TEXT
		END AS transaction_day,

		tpp.company_name, 
		tpp.business_id, 
		tpp.sub_account_name, 
		tpp.account_id, 
		CASE WHEN dm.account_id IS NOT NULL THEN 1 ELSE 0 END AS is_merchant_dm, 
		pcas.label as activity_sector, 
		tpp.authentication_indicator, 
		pcpm.label AS payment_method,
		cc.name AS payment_country, 
		cur.name AS currency, 
		psp.type AS psp, 
		acq.label AS acquirer,
		b.branch as BU,
		eci.label AS ecommerce_indicator, 

		--Indicateurs transactionnels 
		COUNT(tpp.transaction_id) AS trx_volume_created, 
		COUNT(CASE WHEN st.authorized IS NOT NULL OR st.authorized_and_pending IS NOT NULL THEN tpp.transaction_id END) AS trx_volume_authorized,
		COUNT(CASE WHEN st.captured IS NOT NULL THEN tpp.transaction_id END) AS trx_volume_captured, 
		SUM(tpp.transaction_amount * tpp.avg_quotation_value)::numeric (13,2) AS end_user_amount_created, 
		SUM(CASE WHEN st.authorized IS NOT NULL THEN (tpp.transaction_amount * tpp.avg_quotation_value) END)::numeric(13,2) AS end_user_amount_authorized, 
		SUM(CASE WHEN st.captured IS NOT NULL THEN (tpp.transaction_amount * tpp.avg_quotation_value) END)::numeric(13,2) AS end_user_amount_captured 

	FROM t_tpp_transaction tpp
	LEFT JOIN  t_top_marchands_DM dm ON tpp.merchant_id = dm.merchant_id	
	INNER JOIN dimensions.payment_common_payment_method pcpm ON pcpm.id = tpp.payment_method_id
	INNER JOIN dimensions.common_countries cc ON cc.id = tpp.country_id
	INNER JOIN dimensions.common_currencies cur ON cur.id = tpp.devise_id
	INNER JOIN dimensions.payment_common_psp psp ON psp.id = tpp.psp_id
	INNER JOIN dimensions.payment_common_acqueror acq ON acq.id = tpp.acqueror_id
	INNER JOIN scripts.payment_tpp_transaction_status st ON st.transaction_id = tpp.transaction_id
	INNER JOIN dimensions.payment_common_activity_sector pcas ON pcas.id = tpp.activity_sector_id
	INNER JOIN dimensions.payment_common_ecommerce_indicator eci ON (eci.id = tpp.in_ecommerce_indicator_id)
	INNER JOIN reporting_payment.v_merchant_branch_v2 b ON (tpp.merchant_id = b.merchant_id)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18;

	-----------------------------------------------------------------------------------------------------------------------
	--------------------------- 3.2 Insertion des données transactionnelles WALLET  
	-----------------------------------------------------------------------------------------------------------------------   

	RAISE LOG '[%] REPORTING_MONTHLY INSERT TRANSACTION WALLET 1', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');

	DROP TABLE IF EXISTS t_transaction;
	CREATE TEMP TABLE t_transaction ON COMMIT DROP AS
	SELECT
		hpt.transaction_id,
		hpt.transaction_date::date AS transaction_date,
		hpt.transaction_date AS transaction_date_timstamptz,
		hpt.montant_devise,
		hpt.devise_id,
		hpt.merchant_id,
		hpt.country_id,
		hpt.psp_id,
		hpt.acqueror_id,
		hpt.payment_method_id,
	    mer.company_name, 
	   	mer.business_id, 
	   	mer.sub_account_name, 
	   	mer.account_id,
	   	i.avg_quotation_value
	FROM logs.payment_transaction hpt
	INNER JOIN dimensions.payment_applifi_merchants mer ON mer.id = hpt.merchant_id
	INNER JOIN t_intervals_reporting_mensuel i ON (i.start_date_day = hpt.transaction_date::date AND i.base_currency_id = hpt.devise_id) 
	WHERE
		hpt.transaction_date >= v_start AND
		hpt.transaction_date < v_end AND
		hpt.transaction_type_id IN (3,4,12,13,14);

	CREATE INDEX ON t_transaction(transaction_id);
	CREATE INDEX ON t_transaction(transaction_date);
	CREATE INDEX ON t_transaction(devise_id);
	CREATE INDEX ON t_transaction(merchant_id);
	CREATE INDEX ON t_transaction(country_id);
	CREATE INDEX ON t_transaction(psp_id);
	CREATE INDEX ON t_transaction(acqueror_id);
	CREATE INDEX ON t_transaction(payment_method_id);


	RAISE LOG '[%] REPORTING_MONTHLY INSERT TRANSACTION WALLET 2', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');

	INSERT INTO reporting_payment.truncate_insert_reporting_monthly_transaction

	SELECT
		'Professional' AS product,

		CASE WHEN hpt.transaction_date >= v_start_month AND hpt.transaction_date < v_end_month
		THEN TO_CHAR( hpt.transaction_date_timstamptz AT TIME ZONE 'Europe/Paris', 'YYYY-MM'::text) 
		ELSE NULL::TEXT
		END AS transaction_month,

		CASE WHEN hpt.transaction_date >= v_start_week  AND hpt.transaction_date < v_end_week
		THEN TO_CHAR(date_trunc('week', hpt.transaction_date_timstamptz AT TIME ZONE 'Europe/Paris'), 'YYYY-MM-DD'::text) 
		ELSE NULL::TEXT
		END AS transaction_week,

		CASE WHEN hpt.transaction_date >= v_start_day AND hpt.transaction_date < v_end_day
		THEN to_char(hpt.transaction_date_timstamptz AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD'::text)
		ELSE NULL::TEXT 
		END AS transaction_day,

		hpt.company_name, 
		hpt.business_id, 
		hpt.sub_account_name, 
		hpt.account_id, 
		NULL::integer AS is_merchant_dm, 
		NULL::text as activity_sector, 
		NULL::integer AS authentication_indicator, 
		pcpm.label AS payment_method,
		cc.name AS payment_country, 
		cur.name AS currency, 
		psp.type AS psp, 
		acq.label AS acquirer,
		b.branch as BU,
		NULL::text AS ecommerce_indicator, 

		-- Indicators 
		COUNT(hpt.transaction_id) AS trx_volume_created,
		COUNT(CASE WHEN st.captured IS NOT NULL THEN hpt.transaction_id END) AS trx_volume_authorized,
		COUNT(CASE WHEN st.captured IS NOT NULL THEN hpt.transaction_id END) AS trx_volume_captured, 
		SUM(hpt.montant_devise * hpt.avg_quotation_value)::numeric(13,2) AS end_user_amount_created,
		SUM(CASE WHEN st.captured IS NOT NULL THEN (hpt.montant_devise * hpt.avg_quotation_value) END)::numeric(13,2) AS end_user_amount_authorized,
		SUM(CASE WHEN st.captured IS NOT NULL THEN (hpt.montant_devise * hpt.avg_quotation_value) END)::numeric(13,2)  AS end_user_amount_captured

	FROM t_transaction hpt
	INNER JOIN scripts.payment_transaction_status st ON hpt.transaction_id = st.transaction_id
	INNER JOIN dimensions.payment_common_payment_method pcpm ON pcpm.id = hpt.payment_method_id
	INNER JOIN dimensions.common_countries	cc ON cc.id = hpt.country_id
	INNER JOIN dimensions.common_currencies cur ON cur.id = hpt.devise_id
	INNER JOIN dimensions.payment_common_psp psp ON psp.id = hpt.psp_id
	INNER JOIN dimensions.payment_common_acqueror acq ON acq.id = hpt.acqueror_id
	INNER JOIN reporting_payment.v_merchant_branch_v2 b ON (hpt.merchant_id = b.merchant_id)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18; 

	RAISE LOG '[%] REPORTING_MONTHLY END INSERT TRANSACTION', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');


	/************************************************************************************************************************/
	/* 			             		ETAPE 4 : INSERTION DES DONNEES FINANCIERES  			      			*/
	/***********************************************************************************************************************/ 
	-----------------------------------------------------------------------------------------------------------------------
	--------------------------- 4.1 Alimentation de la table finance :
	-----------------------------------------------------------------------------------------------------------------------

	RAISE LOG '[%] REPORTING_MONTHLY BEGIN FONCTION FINANCE', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'); 

	-- Truncate final table 
	TRUNCATE TABLE reporting_payment.truncate_insert_reporting_monthly_financial; 

	RAISE LOG '[%] REPORTING_MONTHLY BEGIN INSERT FINANCIAL', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');

	CREATE TEMP TABLE t_intervals_fi ON COMMIT DROP AS 
	    SELECT
		   inv.id as interval_id,
		   inv.start_date
		FROM dimensions.intervals inv
		WHERE inv.type = 'month' 
		AND inv.start_date >= v_start
		AND inv.start_date < v_end;

	CREATE INDEX ON t_intervals_fi (interval_id);

	INSERT INTO reporting_payment.truncate_insert_reporting_monthly_financial
	SELECT 
		CASE WHEN client_entity =  Wallet' THEN 'Professional' ELSE 'Enterprise' END AS product,
		TO_CHAR (start_date AT TIME ZONE 'Europe/Paris', 'YYYY-MM') AS financial_month, 
		mer.company_name, 
		mer.business_id, 
		mer.sub_account_name, 
		mer.account_id,
		conso.is_transaction_fee::integer AS is_transaction_fee, 
		CASE WHEN opt.label ILIKE '%none%' THEN opt1.label
		ELSE opt.label
		END AS operation_type,
		pcpp.label as payment_product,
		-- ppm.name as payment_method_fi,

		-- Indicateurs
		SUM (sales_in_eur)::numeric(13,0) AS turnover, 
		SUM (payout_in_eur)::numeric(13,0) AS payout , 
		SUM (margin_in_eur)::numeric(13,0) AS margin  

	FROM consolidations.payment_applifi_sales conso 
	INNER JOIN t_intervals_fi i ON i.interval_id = conso.interval_id
	INNER JOIN dimensions.payment_applifi_merchants mer ON conso.merchant_id = mer.id
	INNER JOIN dimensions.payment_common_operation_type opt ON opt.id = conso.payout_operation_type_id
	INNER JOIN dimensions.payment_common_operation_type opt1 ON opt1.id = conso.sales_operation_type_id
	INNER JOIN dimensions.payment_common_payment_product pcpp ON pcpp.id = conso.payment_product_id
	--INNER JOIN dimensions.payment_allopass_price_point_methods pcm ON ppm.id = conso.price_point_method_id 
	WHERE
		conso.client_entity IN ('Wallet', 'TPP')  
	GROUP BY 1,2,3,4,5,6,7,8,9--,10
	; 

	RAISE LOG '[%] REPORTING_MONTHLY END FONCTION', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');

		END; 

	$BODY$;

	ALTER FUNCTION reporting_payment.truncate_insert_reporting_monthly()
	    OWNER TO dw;

DROP view sandbox.v_monthly_reporting_transaction_financial
	--Vue finale
	CREATE OR REPLACE VIEW reporting_payment.v_monthly_reporting_transaction_financial
	AS
	--transaction
	SELECT
		t.product,
		'transaction'::TEXT AS analyse_type,
		t.transaction_month,
		t.transaction_week,
		t.transaction_day,
		t.company_name, 
		t.business_id, 
		t.branch,
		t.sub_account_name, 
		t.account_id,
		t.is_merchant_dm, 
		t.activity_sector,
		t.authentication_indicator, 
		t.payment_method,
		t.payment_country, 
		t.currency, 
		t.psp, 
		t.acquirer,
		t.ecommerce_indicator, 
		t.trx_volume_created, 
		t.trx_volume_authorized,
		t.trx_volume_captured, 
		t.end_user_amount_created, 
		t.end_user_amount_authorized, 
		t.end_user_amount_captured,
		NULL::TEXT AS financial_month, 
		NULL::INTEGER AS is_transaction_fee, 
		NULL::TEXT AS operation_type,
		NULL::TEXT AS payment_product,
		--NULL::TEXT AS payment_method_fi,
		NULL::NUMERIC AS turnover, 
		NULL::NUMERIC AS payout , 
		NULL::NUMERIC AS margin  
	FROM reporting_payment.truncate_insert_reporting_monthly_transaction t

	UNION ALL

	--financial
	SELECT
		f.product,
		'financial'::TEXT AS analyse_type,
		NULL::TEXT AS transaction_month,
		NULL::TEXT AS transaction_week,
		NULL::TEXT AS transaction_day,
		f.company_name, 
		f.business_id, 
		NULL::TEXT AS branch,
		f.sub_account_name, 
		f.account_id,
		NULL::INTEGER AS is_merchant_dm, 
		NULL::TEXT AS activity_sector,
		NULL::INTEGER AS authentication_indicator, 
		NULL::TEXT AS payment_method,
		NULL::TEXT AS payment_country, 
		NULL::TEXT AS currency, 
		NULL::TEXT AS psp, 
		NULL::TEXT AS acquirer,
		NULL::TEXT AS ecommerce_indicator, 
		NULL::INTEGER AS trx_volume_created, 
		NULL::INTEGER AS trx_volume_authorized,
		NULL::INTEGER AS trx_volume_captured, 
		NULL::NUMERIC AS end_user_amount_created, 
		NULL::NUMERIC AS end_user_amount_authorized, 
		NULL::NUMERIC AS end_user_amount_captured,
		f.financial_month, 
		f.is_transaction_fee, 
		f.operation_type,
		f.payment_product,
		--f.payment_method_fi,
		f.turnover, 
		f.payout, 
		f.margin
	FROM reporting_payment.truncate_insert_reporting_monthly_financial f;

	ALTER VIEW reporting_payment.v_monthly_reporting_transaction_financial
	OWNER TO dw; 


--Execuction
\timing
set client_min_messages='LOG';
select reporting_payment.truncate_insert_reporting_monthly();


/*
--Copie DEV
psql -p 5432 -U postgres -h dwhx.reporting.db datawarehouse -c "\copy (select * from sandbox.truncate_insert_reporting_monthly_financial) to stdout"  | psql -p 6432 -U postgres -h dwh.dev datawarehouse  -c "\copy sandbox.truncate_insert_reporting_monthly_financial from stdin"
psql -p 5432 -U postgres -h dwh.reporting.db datawarehouse -c "\copy (select * from sandbox.truncate_insert_reporting_monthly_transaction) to stdout"  | psql -p 6432 -U postgres -h dwh.dev datawarehouse  -c "\copy sandbox.truncate_insert_reporting_monthly_transaction from stdin"


select distinct transaction_month from reporting_payment.v_monthly_reporting_transaction_financial;


*/

1er ticket

changement schémas reporting_payment

2e ticket automatisation

vue tab dédié

pas lundi et (10 ou 11)


select
'view'::text AS type,
v.table_schema as schema,
v.table_schema ||'.' || v.table_name as name
from information_schema.views v
where v.table_schema IN ('vue_tab', 'reporting_payment', 'scripts', 'reporting_bo', 'reporting', 'metadata', 'logs', 'google_analytics', 'export', 'dimensions', 'data_marts', 'crm', 'consolidations')
and (v.view_definition ilike '%truncate_insert_reporting_monthly_transaction%')
UNION ALL
--Function
select
distinct 'function'::text AS type,
r.specific_schema as schema,
r.specific_schema ||'.' || r.routine_name as name
from information_schema.routines r
inner join pg_proc p ON p.proname = r.routine_name
where r.routine_schema IN ('vue_tab', 'reporting_payment', 'scripts', 'reporting_bo', 'reporting', 'metadata', 'logs', 'google_analytics', 'export', 'dimensions', 'data_marts', 'crm', 'consolidations')
and r.routine_type = 'FUNCTION'
and (routine_definition ilike '%truncate_insert_reporting_monthly_transaction%') 
order by 1,2,3;


select
'view'::text AS type,
v.table_schema as schema,
v.table_schema ||'.' || v.table_name as name
from information_schema.views v
where v.table_schema IN ('vue_tab', 'reporting_payment', 'scripts', 'reporting_bo', 'reporting', 'metadata', 'logs', 'google_analytics', 'export', 'dimensions', 'data_marts', 'crm', 'consolidations')
and (v.view_definition ilike '%truncate_insert_reporting_monthly_financial%')
UNION ALL
--Function
select
distinct 'function'::text AS type,
r.specific_schema as schema,
r.specific_schema ||'.' || r.routine_name as name
from information_schema.routines r
inner join pg_proc p ON p.proname = r.routine_name
where r.routine_schema IN ('vue_tab', 'reporting_payment', 'scripts', 'reporting_bo', 'reporting', 'metadata', 'logs', 'google_analytics', 'export', 'dimensions', 'data_marts', 'crm', 'consolidations')
and r.routine_type = 'FUNCTION'
and (routine_definition ilike '%truncate_insert_reporting_monthly_financial%') 
order by 1,2,3;


reporting_payment.v_monthly_reporting_transaction_financial
