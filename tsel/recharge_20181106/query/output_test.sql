SELECT
	r.Channel,
	r.date,
	COALESCE(l1.area, l2.area, mdl.area, 'UNKNOWN') as area,
	COALESCE(l1.region, l2.region, mdl.region, 'UNKNOWN') as region,
	COALESCE(l1.branch, l2.branch, mdl.branch, 'UNKNOWN') as branch,
	COALESCE(l1.subbranch, l2.subbranch, mdl.subbranch, 'UNKNOWN') as subbranch,
	COALESCE(l1.cluster, l2.cluster, mdl.cluster, 'UNKNOWN') as cluster,
	CASE
        WHEN l1.area IS NOT NULL OR l2.area IS NOT NULL THEN 'LACIMA'
        WHEN mdl.area IS NOT NULL THEN 'MOSTDOM'
        ELSE 'UNKNOWN'
	END as lookup,
	digipos_flag,
	SUM(r.amount) as total_amount,
	SUM(r.trx) as total_trx
FROM recharge_test r
LEFT JOIN lacima l1 ON r.LACCI_subscriber_3G = l1.lacci AND l1.node_type ='3G'
LEFT JOIN lacima l2 ON r.LACCI_subscriber_4G = l2.lacci AND l2.node_type ='4G'
    AND l1.lacci IS NULL
LEFT JOIN mostdom_lacima mdl ON r.msisdn = mdl.msisdn
    AND mdl.area != 'UNKNOWN'
    AND l1.lacci IS NULL
    AND l2.lacci IS NULL
WHERE r.node = ''
GROUP BY
	r.Channel,
	r.date,
	COALESCE(l1.area, l2.area, mdl.area, 'UNKNOWN'),
	COALESCE(l1.region, l2.region, mdl.region, 'UNKNOWN'),
	COALESCE(l1.branch, l2.branch, mdl.branch, 'UNKNOWN'),
	COALESCE(l1.subbranch, l2.subbranch, mdl.subbranch, 'UNKNOWN'),
	COALESCE(l1.cluster, l2.cluster, mdl.cluster, 'UNKNOWN'),
	CASE
        WHEN l1.area IS NOT NULL OR l2.area IS NOT NULL THEN 'LACIMA'
        WHEN mdl.area IS NOT NULL THEN 'MOSTDOM'
        ELSE 'UNKNOWN'
	END,
	digipos_flag
UNION ALL
SELECT
	r.Channel,
	r.date,
	COALESCE(l1.area, mdl.area, 'UNKNOWN') as area,
	COALESCE(l1.region, mdl.region, 'UNKNOWN') as region,
	COALESCE(l1.branch, mdl.branch, 'UNKNOWN') as branch,
	COALESCE(l1.subbranch, mdl.subbranch, 'UNKNOWN') as subbranch,
	COALESCE(l1.cluster, mdl.cluster, 'UNKNOWN') as cluster,
	CASE
        WHEN l1.area IS NOT NULL THEN 'LACIMA'
        WHEN mdl.area IS NOT NULL THEN 'MOSTDOM'
        ELSE 'UNKNOWN'
	END as lookup,
	digipos_flag,
	SUM(r.amount) as total_amount,
	SUM(r.trx) as total_trx
FROM recharge_test r
LEFT JOIN lacima l1 ON r.LACCI_subscriber_3G = l1.lacci AND l1.node_type ='3G'
LEFT JOIN mostdom_lacima mdl ON r.msisdn = mdl.msisdn
    AND mdl.area != 'UNKNOWN'
    AND l1.lacci IS NULL
WHERE r.node = '2G' OR r.node = '3G'
GROUP BY
	r.Channel,
	r.date,
	COALESCE(l1.area, mdl.area, 'UNKNOWN'),
	COALESCE(l1.region, mdl.region, 'UNKNOWN'),
	COALESCE(l1.branch, mdl.branch, 'UNKNOWN'),
	COALESCE(l1.subbranch, mdl.subbranch, 'UNKNOWN'),
	COALESCE(l1.cluster, mdl.cluster, 'UNKNOWN'),
	CASE
        WHEN l1.area IS NOT NULL THEN 'LACIMA'
        WHEN mdl.area IS NOT NULL THEN 'MOSTDOM'
        ELSE 'UNKNOWN'
	END,
	digipos_flag
UNION ALL
SELECT
	r.Channel,
	r.date,
	COALESCE(l1.area, mdl.area, 'UNKNOWN') as area,
	COALESCE(l1.region, mdl.region, 'UNKNOWN') as region,
	COALESCE(l1.branch, mdl.branch, 'UNKNOWN') as branch,
	COALESCE(l1.subbranch, mdl.subbranch, 'UNKNOWN') as subbranch,
	COALESCE(l1.cluster, mdl.cluster, 'UNKNOWN') as cluster,
	CASE
        WHEN l1.area IS NOT NULL THEN 'LACIMA'
        WHEN mdl.area IS NOT NULL THEN 'MOSTDOM'
        ELSE 'UNKNOWN'
	END as lookup,
	digipos_flag,
	SUM(r.amount) as total_amount,
	SUM(r.trx) as total_trx
FROM recharge_test r
LEFT JOIN lacima l1 ON r.LACCI_subscriber_4G = l1.lacci AND l1.node_type ='4G'
LEFT JOIN mostdom_lacima mdl ON r.msisdn = mdl.msisdn
    AND mdl.area != 'UNKNOWN'
    AND l1.lacci IS NULL
WHERE r.node = '4G'
GROUP BY
	r.Channel,
	r.date,
	COALESCE(l1.area, mdl.area, 'UNKNOWN'),
	COALESCE(l1.region, mdl.region, 'UNKNOWN'),
	COALESCE(l1.branch, mdl.branch, 'UNKNOWN'),
	COALESCE(l1.subbranch, mdl.subbranch, 'UNKNOWN'),
	COALESCE(l1.cluster, mdl.cluster, 'UNKNOWN'),
	CASE
        WHEN l1.area IS NOT NULL THEN 'LACIMA'
        WHEN mdl.area IS NOT NULL THEN 'MOSTDOM'
        ELSE 'UNKNOWN'
	END,
	digipos_flag