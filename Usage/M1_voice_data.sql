select 
in_account_number,
sum(total_volume_data) as DATA_USAGE_M_1,
	sum(total_national_minutes_outgoing + total_national_minutes_incoming) as NATIONAL_VOICE_USAGE_M_1, 
	sum(total_international_minutes_outgoing + total_international_minutes_incoming) as INTERNATIONAL_VOICE_USAGE_M_1,
	sum(ad_total_revenue_net/30) as ARPU_M_1,
	case 
	when sum(total_volume_data) <= 5 then '5'
	when sum(total_volume_data) <= 10 and sum(total_volume_data) > 5 then '10'
	when sum(total_volume_data) <= 15 and sum(total_volume_data) > 10 then '15'
	when sum(total_volume_data) <= 25 and sum(total_volume_data) > 15 then '25'
	when sum(total_volume_data) <= 50 and sum(total_volume_data) > 25 then '50'
else '>50'
end as Last_month_data_usage,
sum(total_national_minutes_outgoing) as Last_month_outgoing_national_voiceminutes,
sum(total_international_minutes_outgoing) as Last_month_outgoing_international_voiceminutes,
sum(total_roaming_minutes_outgoing) as Last_month_outgoing_roaming_voiceminutes,
sum(total_roaming_minutes_incoming) as Last_month_incoming_roaming_voiceminutes
	from (
SELECT
	ad_date,
	in_account_number,
	sum(ad_total_revenue_net) as ad_total_revenue_net,
	---------------------------------USAGE -----------------------------------------------Data--data usage : national + pay
 SUM(total_data_usage_national + total_data_payg) AS total_data_usage_national,
	--data usage : roaming
 sum(total_data_usage_roaming) as total_data_usage_roaming,
	-- data total usage
 sum(total_volume_data) as total_volume_data,
	------ voice-- Min Nat MOC
 sum(total_national_minutes_outgoing) as total_national_minutes_outgoing ,
	--Min Nat PAY
 sum(total_national_minutes_outgoing_pay) as total_national_minutes_outgoing_pay,
	--Min Nat Bundle
 sum(total_national_minutes_outgoing_bundle) as total_national_minutes_outgoing_bundle,
	--Min Int MOC
 sum(total_international_minutes_outgoing) as total_international_minutes_outgoing ,
 sum(total_roaming_minutes_outgoing) as total_roaming_minutes_outgoing ,
	--Min Int PAY
 sum(total_international_minutes_outgoing_pay) as total_international_minutes_outgoing_pay,
	--Min Int Bundle
 sum(total_international_minutes_outgoing_bundle) as total_international_minutes_outgoing_bundle,
	-- SMS Count
 SUM(total_count_sms) AS total_count_sms ,
	SUM(total_count_sms_nat) AS total_count_sms_nat ,
	SUM(total_count_sms_intl) AS total_count_sms_intl ,
	SUM(total_count_sms_roaming) AS total_count_sms_roaming ,
	-- MTC Minutes
 SUM(total_mins_incoming) AS total_mins_incoming,
	SUM(total_national_minutes_incoming) AS total_national_minutes_incoming,
	SUM(total_international_minutes_incoming) AS total_international_minutes_incoming,
	SUM(total_roaming_minutes_incoming) AS total_roaming_minutes_incoming,
	-- Recharge
 SUM(total_amount_recharge) AS total_amount_recharge,
	-- Data GB Transfer
 SUM(total_data_usage_transfer) AS total_data_usage_transfer
FROM
	(
	SELECT
		ad_date,
		ad_accountnum AS IN_ACCOUNT_NUMBER,
		sum(ad_total_revenue_net) as ad_total_revenue_net,
		-- International Revenue ( Usage and Expired)
 SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MOC' AND traffic_national_international = 'International' THEN (ad_total_revenue_received-ad_total_tax_amount) ELSE 0 END) as total_revenue_net_moc_voice_usage_int,
		SUM(CASE WHEN dev_category_3 IN ('Voice Plan Intl', 'Voice Intl Booster') THEN (ad_total_revenue_received_expired_bundle - ad_total_tax_amount_expired_bundle ) ELSE 0 END) AS total_revenue_net_voice_expired_int,
		-- SUM(CASE WHEN service_group = 'Voice' AND dev_event_id='429'  THEN (ad_total_revenue_received-ad_total_tax_amount) ELSE 0 END) as total_revenue_net_voice_upfront_int,
 SUM(CASE WHEN service_group = 'Voice' AND dev_event_name = 'Booster_IntlMin_Unlimited' THEN (ad_total_revenue_received-ad_total_tax_amount) ELSE 0 END) as total_revenue_net_voice_upfront_int,
		-- National Revenue ( Usage and Expired)
 SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MOC' AND traffic_national_international = 'National' THEN (ad_total_revenue_received-ad_total_tax_amount) ELSE 0 END) as total_revenue_net_moc_voice_usage_nat,
		SUM(CASE WHEN dev_category_3 IN ('Voice Plan Natl', 'Voice Natl Booster') THEN (ad_total_revenue_received_expired_bundle - ad_total_tax_amount_expired_bundle ) ELSE 0 END) AS total_revenue_net_voice_expired_nat,
		-- Roaming Revenue ( Usage and Expired)
 SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MOC' AND traffic_national_international = 'Roaming' THEN (ad_total_revenue_received-ad_total_tax_amount) ELSE 0 END) as total_revenue_net_moc_voice_usage_roaming,
		SUM(CASE WHEN dev_category_3 = 'Roaming Voice Bundle' THEN (ad_total_revenue_received_expired_bundle - ad_total_tax_amount_expired_bundle ) ELSE 0 END) AS total_revenue_net_voice_expired_roaming,
		-- Total Voice Expired
 SUM(CASE WHEN service_group = 'Voice' THEN (ad_total_revenue_received_expired_bundle - ad_total_tax_amount_expired_bundle ) ELSE 0 END) AS total_revenue_net_voice_expired,
		-- National Data Usage and Expired Rev
 SUM(CASE WHEN service_group = 'Data' AND traffic_national_international = 'National' THEN ad_total_revenue_received-ad_total_tax_amount ELSE 0 END) AS total_revenue_net_data_usage_nat,
		SUM(CASE WHEN dev_category_3 IN ('Data Plan' , 'Data Booster') THEN (ad_total_revenue_received_expired_bundle - ad_total_tax_amount_expired_bundle ) ELSE 0 END) AS total_revenue_net_expired_data_nat,
		SUM(CASE WHEN service_group = 'Data' THEN (ad_total_revenue_received_transfer_bundle-ad_total_tax_amount_transfer_bundle) ELSE 0 END) AS total_revenue_net_transfer_data,
		-- Raoming Data Usage and Expired Rev
 SUM(CASE WHEN service_group = 'Data' AND traffic_national_international = 'Roaming' THEN ad_total_revenue_received-ad_total_tax_amount ELSE 0 END) AS total_revenue_net_data_usage_roaming,
		SUM(CASE WHEN dev_category_3 = 'Roaming Data Bundle' THEN (ad_total_revenue_received_expired_bundle - ad_total_tax_amount_expired_bundle ) ELSE 0 END) AS total_revenue_net_expired_data_roaming,
		-- Total Data Usage and Expired Rev
 SUM(CASE WHEN service_group = 'Data' THEN (ad_total_revenue_received_expired_bundle - ad_total_tax_amount_expired_bundle ) ELSE 0 END) AS total_revenue_net_data_expired,
		--USAGE---- Total National Min Outgoing
 SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MOC' AND traffic_national_international = 'National' AND ad_terminationreason IN (0, 6, 7, 8, 10) THEN ad_total_duration / 60000 ELSE 0 END) total_national_minutes_outgoing,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MOC' AND traffic_national_international = 'National' AND ad_terminationreason IN (0, 6, 7, 8, 10) THEN ad_total_duration_payg / 60000 ELSE 0 END) total_national_minutes_outgoing_pay,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MOC' AND traffic_national_international = 'National' AND ad_terminationreason IN (0, 6, 7, 8, 10) AND dcr_category_2 = 'Voice Bundle' THEN ad_total_duration_counter / 60000 ELSE 0 END) total_national_minutes_outgoing_bundle,
		-- Total International Min Outgoing
 SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MOC' AND traffic_national_international = 'International' AND ad_terminationreason IN (0, 6, 7, 8, 10) THEN ad_total_duration / 60000 ELSE 0 END) total_international_minutes_outgoing,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MOC' AND traffic_national_international = 'International' AND ad_terminationreason IN (0, 6, 7, 8, 10) THEN ad_total_duration_payg / 60000 ELSE 0 END) total_international_minutes_outgoing_pay,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MOC' AND traffic_national_international = 'International' AND ad_terminationreason IN (0, 6, 7, 8, 10) AND dcr_category_2 = 'Voice Bundle' THEN ad_total_duration_counter / 60000 ELSE 0 END) total_international_minutes_outgoing_bundle,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MOC' AND traffic_national_international = 'Roaming' AND ad_terminationreason IN (0, 6, 7, 8, 10) THEN ad_total_duration / 60000 ELSE 0 END) AS total_roaming_minutes_outgoing,
		-- Data
 SUM(CASE WHEN service_group = 'Data' THEN ad_total_data_volume / 1024 / 1024 / 1024 ELSE 0 END) AS total_volume_data,
		SUM(CASE WHEN service_group = 'Data' AND traffic_national_international = 'National' THEN ad_total_data_volume_counter / 1024 / 1024 / 1024 ELSE 0 END) AS total_data_usage_national,
		SUM(CASE WHEN service_group = 'Data' THEN ad_total_data_volume_payg / 1024 / 1024 / 1024 ELSE 0 END) AS total_data_payg,
		SUM(CASE WHEN service_group = 'Data' AND traffic_national_international = 'Roaming' THEN ad_total_data_volume_counter / 1024 / 1024 / 1024 ELSE 0 END) AS total_data_usage_roaming,
		SUM(CASE WHEN service_group = 'Data' AND dcr_category_2 = 'Data Bundle' THEN ad_total_data_volume_counter / 1024 / 1024 / 1024 ELSE 0 END) AS total_data_usage_bundle,
		-----------------------------------***  SMS ******************************************---SMS -REVENUE ---
 SUM(CASE WHEN service_group = 'SMS' THEN ad_total_revenue_received-ad_total_tax_amount ELSE 0 END) AS total_revenue_net_sms,
		SUM(CASE WHEN service_group = 'SMS' AND traffic_national_international = 'National' THEN ad_total_revenue_received-ad_total_tax_amount ELSE 0 END) AS total_revenue_net_sms_nat,
		SUM(CASE WHEN service_group = 'SMS' AND traffic_national_international = 'International' THEN ad_total_revenue_received-ad_total_tax_amount ELSE 0 END) AS total_revenue_net_sms_intl,
		SUM(CASE WHEN service_group = 'SMS' AND traffic_national_international = 'Roaming' THEN ad_total_revenue_received-ad_total_tax_amount ELSE 0 END) AS total_revenue_net_sms_roaming,
		---- SMS COUNT
 SUM(CASE WHEN service_group = 'SMS' THEN ad_total_sms ELSE 0 END) AS total_count_sms ,
		SUM(CASE WHEN service_group = 'SMS' AND traffic_national_international = 'National' THEN ad_total_sms ELSE 0 END) AS total_count_sms_nat ,
		SUM(CASE WHEN service_group = 'SMS' AND traffic_national_international = 'International' THEN ad_total_sms ELSE 0 END) AS total_count_sms_intl ,
		SUM(CASE WHEN service_group = 'SMS' AND traffic_national_international = 'Roaming' THEN ad_total_sms ELSE 0 END) AS total_count_sms_roaming ,
		-------------------------------------********************* MTC VOICE **********************************--MTC VOICE REVENUE -----
 SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MTC' THEN ad_total_revenue_received-ad_total_tax_amount ELSE 0 END) AS total_revenue_net_mtc,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MTC' AND traffic_national_international = 'National' THEN ad_total_revenue_received-ad_total_tax_amount ELSE 0 END) AS total_revenue_net_mtc_nat,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MTC' AND traffic_national_international = 'International' THEN ad_total_revenue_received-ad_total_tax_amount ELSE 0 END) AS total_revenue_net_mtc_intl,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MTC' AND traffic_national_international = 'Roaming' THEN ad_total_revenue_received-ad_total_tax_amount ELSE 0 END) AS total_revenue_net_mtc_roaming,
		---- voice MTC Minutes
 SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MTC' AND ad_terminationreason IN (0, 6, 7, 8, 10) THEN ad_total_duration / 60000 ELSE 0 END) AS total_mins_incoming,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MTC' AND traffic_national_international = 'National' AND ad_terminationreason IN (0, 6, 7, 8, 10) THEN ad_total_duration / 60000 ELSE 0 END) AS total_national_minutes_incoming,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MTC' AND traffic_national_international = 'International' AND ad_terminationreason IN (0, 6, 7, 8, 10) THEN ad_total_duration / 60000 ELSE 0 END) AS total_international_minutes_incoming,
		SUM(CASE WHEN service_group = 'Voice' AND service_type = 'MTC' AND traffic_national_international = 'Roaming' AND ad_terminationreason IN (0, 6, 7, 8, 10) THEN ad_total_duration / 60000 ELSE 0 END) AS total_roaming_minutes_incoming,
		--- Recharge
 SUM(CASE WHEN service_group = 'Recharge' THEN ad_total_adjustment_amount ELSE 0 END) AS total_amount_recharge,
		-- Data Transfer Volume GB
 SUM(CASE WHEN dcr_counter_type_name = 'AC.Sender_DataCounter' THEN ((ad_total_counter_data_volume_transfer)/ 1024 / 1024 / 1024) ELSE 0 END ) as total_data_usage_transfer
 -- count(distinct case when fus_service_type_id in (1,2) and fus_call_direction_id = 1 then fus_bno else 0 end) as UNIQUE_NUMBERS_CALLED_TEXTED_TO
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
		to_char(date(ad_date), 'yyyy-mm') in (select to_char(ADD_MONTHS(date('"+context.vLoadDate+"'),-1),'yyyy-mm')) 
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
	1,
	2 )t 
group by 1;	