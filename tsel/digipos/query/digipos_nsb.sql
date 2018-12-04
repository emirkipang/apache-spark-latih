SELECT src.*
FROM digipos_nsb_src src
LEFT JOIN
(
SELECT * FROM digipos_nsb_before
UNION ALL
SELECT * FROM digipos_nsb_after
) ref ON src.REQUEST_ID = ref.REQUEST_ID
WHERE ref.REQUEST_ID IS NULL