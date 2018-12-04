SELECT
    tksparty as msisdn,
    COALESCE(b.area, c.area, 'UNKNOWN') as area,
	COALESCE(b.region, c.region, 'UNKNOWN') as region,
	COALESCE(b.branch, c.branch, 'UNKNOWN') as branch,
	COALESCE(b.subbranch, c.subbranch, 'UNKNOWN') as subbranch,
	COALESCE(b.cluster, c.cluster, 'UNKNOWN') as cluster
FROM mostdom a
LEFT JOIN lacima b ON CONCAT_WS('~',CONCAT(REPEAT('0',5 - LENGTH(a.lac)), a.lac),CONCAT(REPEAT('0',5 - LENGTH(a.ci)), a.ci)) = b.lacci AND b.node_type ='3G'
LEFT JOIN lacima c ON CONCAT_WS('~',CONCAT(REPEAT('0',7 - LENGTH(a.lac)), a.lac),CONCAT(REPEAT('0',3 - LENGTH(a.ci)), a.ci)) = c.lacci AND c.node_type ='4G'
