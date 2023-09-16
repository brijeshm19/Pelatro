CoreDb
SELECT * FROM (
SELECT  t.[ID]
      ,[SCUserPerID]
      ,[TagID]
	  ,ROW_NUMBER() Over (PARTITION BY MSISDN ORDER BY MSISDN,[DateTime] DESC) AS ROW_RANK
      ,[UserTagAttributeID]
      ,[Action]
      ,[DateTime],
	  MSISDN
  FROM [NotificationEngine3.1].[dbo].[SCUserTagHistory] t
  LEFT JOIN [NotificationEngine3.1].dbo.SCUsersChannelsPermission p
  ON t.SCUserPerID = p.ID
  WHERE TagID = 3
  )a
  WHERE ROW_RANK=1;