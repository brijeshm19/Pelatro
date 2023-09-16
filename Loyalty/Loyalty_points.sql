select  cm.IN_ACCOUNT_NUMBER, sum(points) as points from ae_stg.loy_customer_trx_log l
left join ae_stg.loy_dim_customer c on c.ID = l.customer_id
left join ae_prod.customer_master cm on cm.CRM_UID = c.identifier_value
where date(l.CREATE_DATE) >=  date('"+context.vLoadDate+"')-30
and date(l.CREATE_DATE) <=  date('"+context.vLoadDate+"')
group by 1 order by 1
;