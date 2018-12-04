SELECT
	x.Channel,
	x.date,
	COALESCE(mdl.area, x.area) as area,
	COALESCE(mdl.region, x.region) as region,
	COALESCE(mdl.branch, x.branch) as branch,
	COALESCE(mdl.subbranch, x.subbranch) as subbranch,
	COALESCE(mdl.cluster, x.cluster) as cluster,
	CASE
		WHEN mdl.area IS NOT NULL THEN 'MOSTDOM'
		ELSE x.lookup
	END as lookup,
	x.digipos_flag,
	SUM(x.total_amount),
	SUM(x.total_trx)
FROM
(
	SELECT
		r.msisdn,
		r.Channel,
		r.date,
		COALESCE(l1.area, l2.area, 'UNKNOWN') as area,
		COALESCE(l1.region, l2.region, 'UNKNOWN') as region,
		COALESCE(l1.branch, l2.branch, 'UNKNOWN') as branch,
		COALESCE(l1.subbranch, l2.subbranch, 'UNKNOWN') as subbranch,
		COALESCE(l1.cluster, l2.cluster, 'UNKNOWN') as cluster,
		CASE
			WHEN l1.area IS NOT NULL OR l2.area IS NOT NULL THEN 'LACIMA'
			ELSE 'UNKNOWN'
		END as lookup,
		digipos_flag,
		SUM(r.amount) as total_amount,
		SUM(r.trx) as total_trx
	FROM recharge_test r
	LEFT JOIN lacima l1 ON r.LACCI_subscriber_3G = l1.lacci AND l1.node_type ='3G'
	LEFT JOIN lacima l2 ON r.LACCI_subscriber_4G = l2.lacci AND l2.node_type ='4G'
		AND l1.lacci IS NULL
	WHERE r.node = ''
	GROUP BY
		r.msisdn,
		r.Channel,
		r.date,
		COALESCE(l1.area, l2.area, 'UNKNOWN'),
		COALESCE(l1.region, l2.region, 'UNKNOWN'),
		COALESCE(l1.branch, l2.branch, 'UNKNOWN'),
		COALESCE(l1.subbranch, l2.subbranch, 'UNKNOWN'),
		COALESCE(l1.cluster, l2.cluster, 'UNKNOWN'),
		CASE
			WHEN l1.area IS NOT NULL OR l2.area IS NOT NULL THEN 'LACIMA'
			ELSE 'UNKNOWN'
		END,
		digipos_flag
	UNION ALL
	SELECT
		r.msisdn,
		r.Channel,
		r.date,
		COALESCE(l1.area, 'UNKNOWN') as area,
		COALESCE(l1.region, 'UNKNOWN') as region,
		COALESCE(l1.branch, 'UNKNOWN') as branch,
		COALESCE(l1.subbranch, 'UNKNOWN') as subbranch,
		COALESCE(l1.cluster, 'UNKNOWN') as cluster,
		CASE
			WHEN l1.area IS NOT NULL THEN 'LACIMA'
			ELSE 'UNKNOWN'
		END as lookup,
		digipos_flag,
		SUM(r.amount) as total_amount,
		SUM(r.trx) as total_trx
	FROM recharge_test r
	LEFT JOIN lacima l1 ON r.LACCI_subscriber_3G = l1.lacci AND l1.node_type ='3G'
	WHERE r.node = '2G' OR r.node = '3G'
	GROUP BY
		r.msisdn,
		r.Channel,
		r.date,
		COALESCE(l1.area, 'UNKNOWN'),
		COALESCE(l1.region, 'UNKNOWN'),
		COALESCE(l1.branch, 'UNKNOWN'),
		COALESCE(l1.subbranch, 'UNKNOWN'),
		COALESCE(l1.cluster, 'UNKNOWN'),
		CASE
			WHEN l1.area IS NOT NULL THEN 'LACIMA'
			ELSE 'UNKNOWN'
		END,
		digipos_flag
	UNION ALL
	SELECT
		r.msisdn,
		r.Channel,
		r.date,
		COALESCE(l1.area, 'UNKNOWN') as area,
		COALESCE(l1.region, 'UNKNOWN') as region,
		COALESCE(l1.branch, 'UNKNOWN') as branch,
		COALESCE(l1.subbranch, 'UNKNOWN') as subbranch,
		COALESCE(l1.cluster, 'UNKNOWN') as cluster,
		CASE
			WHEN l1.area IS NOT NULL THEN 'LACIMA'
			ELSE 'UNKNOWN'
		END as lookup,
		digipos_flag,
		SUM(r.amount) as total_amount,
		SUM(r.trx) as total_trx
	FROM recharge_test r
	LEFT JOIN lacima l1 ON r.LACCI_subscriber_4G = l1.lacci AND l1.node_type ='4G'
	WHERE r.node = '4G'
	GROUP BY
		r.msisdn,
		r.Channel,
		r.date,
		COALESCE(l1.area, 'UNKNOWN'),
		COALESCE(l1.region, 'UNKNOWN'),
		COALESCE(l1.branch, 'UNKNOWN'),
		COALESCE(l1.subbranch, 'UNKNOWN'),
		COALESCE(l1.cluster, 'UNKNOWN'),
		CASE
			WHEN l1.area IS NOT NULL THEN 'LACIMA'
			ELSE 'UNKNOWN'
		END,
		digipos_flag
) x LEFT JOIN mostdom_lacima mdl ON x.msisdn = mdl.msisdn AND x.lookup = 'UNKNOWN'
 GROUP BY
	x.Channel,
	x.date,
	COALESCE(mdl.area, x.area),
	COALESCE(mdl.region, x.region),
	COALESCE(mdl.branch, x.branch),
	COALESCE(mdl.subbranch, x.subbranch),
	COALESCE(mdl.cluster, x.cluster),
	CASE
		WHEN mdl.area IS NOT NULL THEN 'MOSTDOM'
		ELSE x.lookup
	END,
	x.digipos_flag