select x.* from (select 
Email	REG_EMAIL,
DateTime,
CustomerID,
AccountNumber as CRMAccountNo,
IsEmailVerified as EMAIL_VERIFIED_FLAG,
ChannelID as EMAIL_REGISTRATION_CHANNEL
from ae_stg.vmwsc_registration  
where IsEmailVerified =1
and DateTime>='2023-01-01'
and DateTime<'2023-03-21')x 
left join (select 
CRMAccountNo
from ae_prod.cas_tx_activations )y on(x.CRMAccountNo=y.CRMAccountNo)
where y.CRMAccountNo is null
order by 2 desc;