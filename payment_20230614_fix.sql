select
	accountnum as IN_ACCOUNT_NUMBER,
	sum(case when Status = 'Success' and x.CreationDate >=( (EXTRACT(MONTH FROM date('2023-06-13')-1) || '/1/' || EXTRACT(YEAR FROM date('2023-06-13')-1))::date ) then amount else 0 end) PAYMENT_TOTAL_AMOUNT_M_0,
	sum(case when Status = 'Success' and x.CreationDate >= ADD_MONTHS((EXTRACT(MONTH FROM date('2023-06-13')-1) || '/1/' || EXTRACT(YEAR FROM date('2023-06-13')-1))::date,-1)
and x.CreationDate<ADD_MONTHS((EXTRACT(MONTH FROM date('2023-06-13')-1) || '/1/' || EXTRACT(YEAR FROM date('2023-06-13')-1))::date, 0)
then amount else 0 end) PAYMENT_TOTAL_AMOUNT_M_1,
	sum(case when Status = 'Success' and x.CreationDate >= ADD_MONTHS((EXTRACT(MONTH FROM date('2023-06-13')-1) || '/1/' || EXTRACT(YEAR FROM date('2023-06-13')-1))::date,-2)
and x.CreationDate<ADD_MONTHS((EXTRACT(MONTH FROM date('2023-06-13')-1) || '/1/' || EXTRACT(YEAR FROM date('2023-06-13')-1))::date,-1)
then amount else 0 end) PAYMENT_TOTAL_AMOUNT_M_2,
	sum(case when Status = 'Failed' and x.CreationDate >=( (EXTRACT(MONTH FROM date('2023-06-13')-1) || '/1/' || EXTRACT(YEAR FROM date('2023-06-13')-1))::date ) then count else 0 end) PAYMENT_COUNT_FAILED_M_0,
	sum(case when Status = 'Failed' and x.CreationDate >= ADD_MONTHS((EXTRACT(MONTH FROM date('2023-06-13')-1) || '/1/' || EXTRACT(YEAR FROM date('2023-06-13')-1))::date,-1)
and x.CreationDate<ADD_MONTHS((EXTRACT(MONTH FROM date('2023-06-13')-1) || '/1/' || EXTRACT(YEAR FROM date('2023-06-13')-1))::date, 0)
then count else 0 end) PAYMENT_COUNT_FAILED_M_1,
	sum(case when Status = 'Failed' and x.CreationDate >= ADD_MONTHS((EXTRACT(MONTH FROM date('2023-06-13')-1) || '/1/' || EXTRACT(YEAR FROM date('2023-06-13')-1))::date,-2)
and x.CreationDate<ADD_MONTHS((EXTRACT(MONTH FROM date('2023-06-13')-1) || '/1/' || EXTRACT(YEAR FROM date('2023-06-13')-1))::date,-1)
then count else 0 end) PAYMENT_COUNT_FAILED_M_2,
	sum(case when Status = 'Success' and x.CreationDate >= cast(date('2023-06-13')-31 as date) then amount else 0 end)PAYMENT_TOTAL_AMOUNT_last_30_days,
	sum(case when Status = 'Success' and x.CreationDate >= cast(date('2023-06-13')-61 as date) and x.CreationDate<cast(date('2023-06-13')-31 as date) then amount else 0 end)PAYMENT_TOTAL_AMOUNT_last_31_60_days,
	sum(case when Status = 'Success' and x.CreationDate >= cast(date('2023-06-13')-91 as date) and x.CreationDate<cast(date('2023-06-13')-61 as date) then amount else 0 end)PAYMENT_TOTAL_AMOUNT_last_61_90_days,
	sum(case when Status = 'Failed' and x.CreationDate >= cast(date('2023-06-13')-31 as date) then count else 0 end)PAYMENT_COUNT_FAILED_last_30_days,
	sum(case when Status = 'Failed' and x.CreationDate >= cast(date('2023-06-13')-61 as date) and x.CreationDate<cast(date('2023-06-13')-31 as date) then count else 0 end)PAYMENT_COUNT_FAILED_last_31_60_days,
	sum(case when Status = 'Failed' and x.CreationDate >= cast(date('2023-06-13')-91 as date) and x.CreationDate<cast(date('2023-06-13')-61 as date) then count else 0 end)PAYMENT_COUNT_FAILED_last_61_90_days,
	max(case when rnk_ = 1 then ChannelName end ) PAYMENT_METHOD
from
	(
	select
		cast(a.CreationDate as date) CreationDate,
		z.msisdn,
		z.accountnum ,
		case
			when b.Status = 'PaymentRefunded' then 'Refund'
			when b.Status = 'PaymentConfirmed' then 'Success'
			else 'Failed'
		end as Status,
		CASE
			when c.ChannelName = 'Spotii' then 'SPOTII'
			when c.ChannelName = 'ApplePayCyberSource' then 'APPLE_PAY'
			when c.ChannelName = 'COD' then 'COD'
			when c.ChannelName = 'POD' then 'POD'
			when c.ChannelName = 'CreditCardCyberSource' then 'CYBER_SOURCE'
			when c.ChannelName = 'POS' then 'POS'
			else c.ChannelName
		END as ChannelName ,
		a.Amount amount,
		1 count,
		row_number() over (partition by accountnum
	order by
		a.CreationDate desc) as rnk_
	from
		ae_stg.payment_paymenttransactions a
	join ae_stg.payment_paymentstatus b on
		(a.StatusID = b.ID)
	join ae_stg.payment_paymentchannel c on
		(a.PaymentChannelID = c.ID)
	join ae_prod.ref_payment_error d on
		(a.ErrorID = d.payment_error_id)
	join ae_stg.payment_paymentdetails e on
		(a.ID = e.PaymentTransactionID)
	join ae_stg.payment_servicetypes f on
		(e.ServiceTypeID = f.ID)
	join ae_prod.ref_payment_method g on
		(a.PaymentMethodID = g.payment_method_id)
	join (
		select
			*
		from
			(
			select
				phonenumber,
				accountnum ,
				min(snapshotdate) min_date,
				max(snapshotdate)max_date
			from
				ae_stg.nobill_snapshot_subscription
			group by
				phonenumber ,
				accountnum )x
		join (
			select
				IN_ACCOUNT_NUMBER ,
				MSISDN ,
				FULL_ACTIVATION_DATE ,
				CRM_UID
			from
				ae_prod.customer_master)y on
			(cast(x.phonenumber as VARCHAR2) = y.MSISDN
				and x.accountnum = y.IN_ACCOUNT_NUMBER
				and cast(y.FULL_ACTIVATION_DATE as date)>= x.min_date
					and cast(y.FULL_ACTIVATION_DATE as date)<x.max_date
))z on
		(a.Uid = z.CRM_UID
			and a.CreationDate >= z.min_date
			and a.CreationDate<z.max_date)
	where
		a.CreationDate >= cast(date('2023-06-13')-91 as date)
		and a.CreationDate <cast(date('2023-06-13') as date)
		and c.ChannelName <> 'Voucher'
)x
group by
	accountnum
;