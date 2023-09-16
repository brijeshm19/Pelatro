select c.IN_ACCOUNT_NUMBER from ae_prod.customer_master c
inner join 
(SELECT 
MSISDN FROM (
SELECT  t.ID
      ,SCUserPerID
      ,TagID
	  ,ROW_NUMBER() Over (PARTITION BY MSISDN ORDER BY MSISDN,t.DateTime DESC) AS ROW_RANK
      ,UserTagAttributeID
      ,Action
	 ,MSISDN
  FROM ae_stg.ne_scusertaghistory t
  LEFT JOIN ae_stg.ne_scuserschannelspermission p
  ON t.SCUserPerID = p.ID
  WHERE TagID = 3
  )a
  WHERE ROW_RANK=1
 )x on x.MSISDN = c.MSISDN;