select cm.IN_ACCOUNT_NUMBER, t.name_en from (
select *, ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY CREATE_DATE desc) as RNK from ae_stg.loy_customer_cashback c where 
tier_id is not null
)x 
left join ae_stg.loy_tier t on x.tier_id = t.ID
left join ae_stg.loy_dim_customer cu on cu.ID = x.customer_id
left join ae_prod.customer_master cm on cm.CRM_UID = cu.identifier_value
where RNK = 1
;