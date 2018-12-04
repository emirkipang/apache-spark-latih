SELECT
	substr(Recharge_Date,0,12) as date,
	COALESCE(b.channel,'MKIOS_VAS_BULK') as Channel,
	CONCAT_WS('~',substr(LACCI_RS,0,5),substr(LACCI_RS,6,5)) as LACCI_RS_3G,
	CONCAT_WS('~',substr(LACCI_RS,0,7),substr(LACCI_RS,8,3)) as LACCI_RS_4G,
	CONCAT_WS('~',substr(LACCI_subscriber,0,5),substr(LACCI_subscriber,6,5)) as LACCI_subscriber_3G,
	CONCAT_WS('~',substr(LACCI_subscriber,0,7),substr(LACCI_subscriber,8,3)) as LACCI_subscriber_4G,
	Price as amount,
	CONCAT_WS('','62',MSISDN) as msisdn,
	CONCAT_WS('~',CONCAT_WS('','62',Hand_phone_No_of_a_Dealer),CONCAT_WS('','62',MSISDN)) as anumber_bnumber,
	'1' as trx
FROM mkios a LEFT JOIN (SELECT DISTINCT type, channel FROM splitcode_mkios) b ON a.recharge_type = b.type
WHERE Response_state = 'O00'
UNION ALL
SELECT
	substr(regexp_replace(regexp_replace(regexp_replace(CreateDate,':',''),'-',''),' ',''),0,12) as date,
	COALESCE(b.channel,'MKIOS_VAS_BULK') as Channel,
	'00000~00000' as LACCI_RS_3G,
	'0000000~000' as LACCI_RS_4G,
	CONCAT_WS('~',substr(LacCi,0,5),substr(LacCi,6,5)) as LacCi_3G,
	CONCAT_WS('~',substr(LacCi,0,7),substr(LacCi,8,3)) as LacCi_4G,
	Credit as amount,
	RecipientUserId as msisdn,
	'' as anumber_bnumber,
	'1' as trx
FROM urp a LEFT JOIN (SELECT DISTINCT type, channel FROM splitcode_urp) b ON a.VoucherType = b.type
WHERE OperState = '61'