select
	cast(
		sysdate-1 as date
	) as load_date,
	IN_ACCOUNT_NUMBER ,
	sum(case when VOUCHER_REDEEM_DATE >= cast(sysdate-31 as date) then VOUCHER_REDEEM_AMOUNT else 0 end ) as VOUCHERS_TOTAL_VALUE_REDEEMED,
	sum(case when VOUCHER_REDEEM_DATE >= cast(sysdate-31 as date) then 1 else 0 end ) as VOUCHERS_TOTAL_COUNT,
	cast(
		sysdate as date
	)-max(case when rnk_ = 1 then VOUCHER_REDEEM_DATE end) as VOUCHER_LAST_REDEMPTION_DATE,
	sum(case when rnk_ = 1 then VOUCHER_REDEEM_AMOUNT else 0 end) as VOUCHER_LAST_REDEEMED_AMOUNT
from
	(
		SELECT
			cast(
				VOUCHER_REDEEM_TIMESTAMP as date
			) as VOUCHER_REDEEM_DATE,
			VOUCHER_REDEEM_IN_ACCOUNT_NUMBER as IN_ACCOUNT_NUMBER,
			VOUCHER_REDEEM_AMOUNT,
			row_number() over (
				partition by VOUCHER_REDEEM_IN_ACCOUNT_NUMBER
			order by
				VOUCHER_REDEEM_TIMESTAMP desc
			) as rnk_
		FROM
			(
				SELECT
					adjustment.fad_vouchernum AS VOUCHER_REDEEM_ADJ_ID,
					adj_lookup.dad_adjustmenttypename AS VOUCHER_REDEEM_ADJ_TYPE,
					adj_lookup.dad_adjustmentreasonname AS VOUCHER_REDEEM_ADJ_REASON,
					7 AS VOUCHER_REDEEM_ACTIONID,
					adj_lookup.dad_adjustmentreasonname AS VOUCHER_REDEEM_TYPE,
					'voucher used'::varchar(12) AS VOUCHER_REDEEM_ACTION_DESC,
					adjustment.fad_quantity AS VOUCHER_REDEEM_QUANTITY,
					adjustment.fad_time_stamp AS VOUCHER_REDEEM_TIMESTAMP,
					(adjustment.fad_time_stamp)::date AS VOUCHER_REDEEM_DATE,
					adjustment.fad_accountnum AS VOUCHER_REDEEM_IN_ACCOUNT_NUMBER,
					adjustment.fad_msisdn AS VOUCHER_REDEEM_MSISDN,
					adjustment.fad_referenceid AS PAYMENT_MERCHANT_REFERENCE,
					adjustment.fad_msisdn AS PAYMENT_CARD_BIN,
					adjustment.fad_msisdn AS PAYMENT_CARD_NUMBER,
					adjustment.fad_amount AS VOUCHER_REDEEM_AMOUNT,
					adjustment.fad_vouchernum AS VOUCHER_REDEEM_SERIAL_NUMBER,
					adjustment.fad_postbalance AS VOUCHER_REDEEM_POST_BALANCE,
					CAS.PAY_PAYMENT_UID,
					CAS.PAY_IN_ACCOUNT_NUMBER,
					*
				FROM
					(
						(
							ae_prod.nobill_fact_adjustment adjustment
						LEFT JOIN ae_prod.nobill_dim_adjustment_type adj_lookup ON
							(
								(
									adjustment.fad_adjustment_type_id = adj_lookup.dad_adjustment_type_id
								)
							)
						)
					LEFT JOIN (
							SELECT
								a.CAS_CRM_ACCOUNT_NUMBER AS PAY_PAYMENT_UID,
								max(a.CAS_IN_ACCOUNT_NUMBER) AS PAY_IN_ACCOUNT_NUMBER
							FROM
								(
									SELECT
										ACTIVATIONS.CRMAccountNo AS CAS_CRM_ACCOUNT_NUMBER,
										ACTIVATIONS.NobillAccountNo AS CAS_IN_ACCOUNT_NUMBER
									FROM
										ae_prod.cas_tx_activations ACTIVATIONS
								) a
							GROUP BY
								a.CAS_CRM_ACCOUNT_NUMBER
						) CAS ON
						(
							(
								CAS.PAY_IN_ACCOUNT_NUMBER = adjustment.fad_accountnum
							)
						)
					)
				WHERE
					(
						(
							adj_lookup.dad_adjustmenttypename = 'voucher'::varchar(7)
						)
							AND (
								(adjustment.fad_time_stamp)::date >= cast(
									sysdate-180 as date
								)
							)
								AND (
									(adjustment.fad_time_stamp)::date < cast(
										sysdate as date
									)
								)
					)
			) t
		WHERE
			(
				t.VOUCHER_REDEEM_ACTIONID = 7
			)
	)x
group by
	IN_ACCOUNT_NUMBER