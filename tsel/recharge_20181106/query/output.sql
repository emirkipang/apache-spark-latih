SELECT
	r.Channel,
	r.date,
	COALESCE(l1.area, l2.area, mdl.area, l1_.area, l2_.area, 'UNKNOWN') as area,
	COALESCE(l1.region, l2.region, mdl.region, l1_.region, l2_.region, 'UNKNOWN') as region,
	COALESCE(l1.branch, l2.branch, mdl.branch, l1_.branch, l2_.branch, 'UNKNOWN') as branch,
	COALESCE(l1.subbranch, l2.subbranch, mdl.subbranch, l1_.subbranch, l2_.subbranch, 'UNKNOWN') as subbranch,
	COALESCE(l1.cluster, l2.cluster, mdl.cluster, l1_.cluster, l2_.cluster, 'UNKNOWN') as cluster,
	CASE
        WHEN l1.area IS NOT NULL OR l2.area IS NOT NULL THEN 'LACIMA'
        WHEN mdl.area IS NOT NULL THEN 'MOSTDOM'
        WHEN l1_.area IS NOT NULL OR l2_.area IS NOT NULL THEN 'DIGIPOS'
        ELSE 'UNKNOWN'
	END as lookup,
	SUM(r.amount) as total_amount,
	SUM(r.trx) as total_trx
FROM recharge r
LEFT JOIN lacima l1 ON r.LACCI_subscriber_3G = l1.lacci AND l1.node_type ='3G'
LEFT JOIN lacima l2 ON r.LACCI_subscriber_4G = l2.lacci AND l2.node_type ='4G'
LEFT JOIN mostdom_lacima mdl ON r.msisdn = mdl.msisdn AND mdl.area != 'UNKNOWN'
LEFT JOIN (SELECT DISTINCT
    date,
    CONCAT_WS('','62',anumber) as anumber,
    CONCAT_WS('','62',substr(bnumber, 2, length(bnumber))) as bnumber,
    CONCAT_WS('~',CONCAT_WS('','62',anumber),CONCAT_WS('','62',substr(bnumber, 2, length(bnumber)))) as anumber_bnumber
 FROM digipos) digi
ON r.anumber_bnumber = digi.anumber_bnumber AND substr(r.date,0,8) = digi.date
LEFT JOIN lacima l1_ ON r.LACCI_RS_3G = l1_.lacci AND l1_.node_type ='3G' AND digi.anumber_bnumber IS NOT NULL
LEFT JOIN lacima l2_ ON r.LACCI_RS_4G = l2_.lacci AND l2_.node_type ='4G' AND digi.anumber_bnumber IS NOT NULL
GROUP BY
	r.Channel,
	r.date,
	COALESCE(l1.area, l2.area, mdl.area, l1_.area, l2_.area, 'UNKNOWN'),
	COALESCE(l1.region, l2.region, mdl.region, l1_.region, l2_.region, 'UNKNOWN'),
	COALESCE(l1.branch, l2.branch, mdl.branch, l1_.branch, l2_.branch, 'UNKNOWN'),
	COALESCE(l1.subbranch, l2.subbranch, mdl.subbranch, l1_.subbranch, l2_.subbranch, 'UNKNOWN'),
	COALESCE(l1.cluster, l2.cluster, mdl.cluster, l1_.cluster, l2_.cluster, 'UNKNOWN'),
	CASE
        WHEN l1.area IS NOT NULL OR l2.area IS NOT NULL THEN 'LACIMA'
        WHEN mdl.area IS NOT NULL THEN 'MOSTDOM'
        WHEN l1_.area IS NOT NULL OR l2_.area IS NOT NULL THEN 'DIGIPOS'
        ELSE 'UNKNOWN'
	END