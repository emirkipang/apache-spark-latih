SELECT src.*
FROM trans_record_src src
LEFT JOIN
(
SELECT * FROM trans_record_before
UNION ALL
SELECT * FROM trans_record_after
) ref ON src.orderid = ref.orderid
WHERE ref.orderis IS NULL