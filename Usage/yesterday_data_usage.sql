select 
in_account_number,
sum(total_volume_data) as PREVIOUS_DAY_DATA_USAGE
	from (
SELECT
	in_account_number,
	-- data total usage
 sum(total_volume_data) as total_volume_data
FROM
	(
	SELECT
		ad_date,
		ad_accountnum AS IN_ACCOUNT_NUMBER,
		-- Data
 SUM(CASE WHEN service_group = 'Data' THEN ad_total_data_volume / 1024 / 1024 / 1024 ELSE 0 END) AS total_volume_data
	FROM
		"+context.connection_verticadb_schema_prod+".nobill_aggr_ws_cost_v3 c
	LEFT JOIN "+context.connection_verticadb_schema_prod+".nobill_dim_counter_type CC ON
		CC.dcr_counter_type_id = C.ad_counter_type_id
	LEFT JOIN (
		SELECT
			dev_eventid_base as dev_event_id,
			max(dev_category_1) dev_category_1,
			max(dev_category_3) dev_category_3,
			MAX(dev_event_name) AS dev_event_name
		FROM
			"+context.connection_verticadb_schema_prod+".nobill_dim_event
		GROUP BY
			1
		ORDER BY
			1)DEV ON
		c.ad_eventid = DEV.dev_event_id
	LEFT JOIN "+context.connection_verticadb_schema_prod+".nobill_dim_transaction_key t on
		t.transaction_id = c.ad_transaction_id
	where
		date(ad_date) = date('"+context.vLoadDate+"')
		AND cc.dcr_category_8 :: INT in(0, 1)
		AND to_char(ad_accountnum) not in (
		select
			distinct accountnum
		from
			"+context.connection_verticadb_schema_prod+".nobill_dim_test_accounts
		WHERE
			accountnum != ''
			AND exclusion_flag = 'INCLUDE')
	group by
		1,
		2)Final_group
group by
	1 )t 
group by 1;	