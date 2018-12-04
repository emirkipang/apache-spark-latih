SELECT
	substr(Recharge_Date,0,12) as date,
	COALESCE(b.channel,'MKIOS_VAS_BULK') as Channel,
	CASE
	    WHEN digi.anumber_bnumber IS NOT NULL THEN CONCAT_WS('~',substr(LACCI_RS,0,5),substr(LACCI_RS,6,5))
	    ELSE CONCAT_WS('~',substr(LACCI_subscriber,0,5),substr(LACCI_subscriber,6,5))
	END as LACCI_subscriber_3G,
	CASE
	    WHEN digi.anumber_bnumber IS NOT NULL THEN CONCAT_WS('~',substr(LACCI_RS,0,7),substr(LACCI_RS,8,3))
	    ELSE CONCAT_WS('~',substr(LACCI_subscriber,0,7),substr(LACCI_subscriber,8,3))
	END as LACCI_subscriber_4G,
	Price as amount,
	CONCAT_WS('','62',MSISDN) as msisdn,
	AD_MSISDN as ad_msisdn,
	'1' as trx,
	'' as node,
	CASE
	    WHEN digi.anumber_bnumber IS NOT NULL THEN 1
	    ELSE 0
	END as digipos_flag
FROM mkios a
LEFT JOIN (
    SELECT
        date,
        CONCAT_WS('~',anumber, bnumber) as anumber_bnumber
    FROM
    (
        SELECT DISTINCT
            date,
            CASE
            WHEN substr(anumber, 1, 1) = '0' THEN CONCAT_WS('','62',substr(anumber, 2))
            ELSE CONCAT_WS('','62',anumber)
            END as anumber,
            CASE
            WHEN substr(bnumber, 1, 1) = '0' THEN CONCAT_WS('','62',substr(bnumber, 2))
            ELSE CONCAT_WS('','62',bnumber)
            END as bnumber
        FROM digipos
    ) a
) digi ON
    CONCAT_WS('~',CONCAT_WS('','62',a.Hand_phone_No_of_a_Dealer),CONCAT_WS('','62',a.MSISDN)) = digi.anumber_bnumber
    AND substr(substr(Recharge_Date,0,12),0,8) = digi.date
    AND TRIM(a.LACCI_subscriber) = ''
LEFT JOIN (SELECT DISTINCT type, channel FROM splitcode_mkios) b ON a.recharge_type = b.type
WHERE Response_state = 'O00'
UNION ALL
SELECT
	substr(regexp_replace(regexp_replace(regexp_replace(CreateDate,':',''),'-',''),' ',''),0,12) as date,
	COALESCE(b.channel,'MKIOS_VAS_BULK') as Channel,
	CONCAT_WS('~',substr(LacCi,0,5),substr(LacCi,6,5)) as LacCi_3G,
	CONCAT_WS('~',substr(LacCi,0,7),substr(LacCi,8,3)) as LacCi_4G,
	Credit as amount,
	RecipientUserId as msisdn,
	'' as ad_msisdn,
	'1' as trx,
    Node as node,
    0 as digipos_flag
FROM urp a LEFT JOIN (SELECT DISTINCT type, channel FROM splitcode_urp) b ON a.VoucherType = b.type
WHERE OperState = '61'