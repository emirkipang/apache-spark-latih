SELECT
    x.date,
    y.node_type,
    COALESCE(y.area, "UNKNOWN"),
    COALESCE(y.region, "UNKNOWN"),
    COALESCE(y.branch, "UNKNOWN"),
    COALESCE(y.subbranch, "UNKNOWN"),
    COALESCE(y.cluster, "UNKNOWN"),
    SUM(x.payload)
FROM
(
    SELECT
       CONCAT(substr(Time,0,13),':00:00') as date,
       CASE
            WHEN CGI != '' THEN CONCAT_WS("~",substr(CGI,6,5),substr(CGI,11,5))
            WHEN CGI = '' AND SAI != '' THEN CONCAT_WS("~",substr(SAI,6,5),substr(SAI,11,5))
            WHEN CGI = '' AND SAI = '' THEN substr(ECGI,6)
            ELSE ''
       END as lacci_or_eci,
       TRIM(QuotaUsage) as payload
    FROM upcc
    WHERE
        (TriggerType = '2' OR TriggerType = '3')
        AND (TRIM(QuotaUsage) != '' AND TRIM(QuotaUsage) != '0')
) x
LEFT JOIN lacima y ON x.lacci_or_eci = y.lacci
GROUP BY
    x.date,
    y.node_type,
    COALESCE(y.area, "UNKNOWN"),
    COALESCE(y.region, "UNKNOWN"),
    COALESCE(y.branch, "UNKNOWN"),
    COALESCE(y.subbranch, "UNKNOWN"),
    COALESCE(y.cluster, "UNKNOWN")