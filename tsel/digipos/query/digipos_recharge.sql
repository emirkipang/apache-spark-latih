SELECT src.*
FROM digipos_recharge_src src
LEFT JOIN
(
SELECT * FROM digipos_recharge_before
UNION ALL
SELECT * FROM digipos_recharge_after
) ref ON src.RECHARGE_ID = ref.RECHARGE_ID
WHERE ref.RECHARGE_ID IS NULL