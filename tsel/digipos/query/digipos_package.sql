SELECT src.*
FROM digipos_package_src src
LEFT JOIN
(
SELECT * FROM digipos_package_before
UNION ALL
SELECT * FROM digipos_package_after
) ref ON src.ACTIVATION_ID = ref.ACTIVATION_ID
WHERE ref.ACTIVATION_ID IS NULL