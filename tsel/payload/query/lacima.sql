SELECT
	CONCAT_WS('~',CONCAT(REPEAT('0',5 - LENGTH(lac)), lac),CONCAT(REPEAT('0',5 - LENGTH(cell_id)), cell_id)) as lacci,
	CASE
	WHEN regional_channel = 'BALI NUSRA' THEN 'BALINUSRA'
	WHEN regional_channel = 'JATENG-DIY' THEN 'JATENG'
	WHEN regional_channel = 'MALUKU DAN PAPUA' THEN 'PUMA'
	WHEN regional_channel = 'SAD REGIONAL' THEN 'UNKNOWN'
	ELSE regional_channel
	END as region,
	branch as branch,
	subbranch as subbranch,
	cluster as cluster,
	CASE
	WHEN regional_channel = 'SUMBAGSEL' OR regional_channel = 'SUMBAGUT' OR regional_channel = 'SUMBAGTENG' THEN 'AREA 1'
	WHEN regional_channel = 'WESTERN JABOTABEK' OR regional_channel = 'EASTERN JABOTABEK' OR regional_channel = 'CENTRAL JABOTABEK' OR regional_channel = 'JABAR' THEN 'AREA 2'
	WHEN regional_channel = 'JATENG-DIY' OR regional_channel = 'JATIM' OR regional_channel = 'BALI NUSRA' THEN 'AREA 3'
	WHEN regional_channel = 'KALIMANTAN' OR regional_channel = 'SULAWESI' OR regional_channel = 'MALUKU DAN PAPUA' THEN 'AREA 4'
	ELSE 'UNKNOWN'
	END as area,
	node as node_type
FROM lacima_3g
WHERE regional_channel != ''
UNION ALL
SELECT
	CONCAT(REPEAT('0',9 - LENGTH(eci)), eci) as lacci,
	CASE
	WHEN regional_channel = 'BALI NUSRA' THEN 'BALINUSRA'
	WHEN regional_channel = 'JATENG-DIY' THEN 'JATENG'
	WHEN regional_channel = 'MALUKU DAN PAPUA' THEN 'PUMA'
	WHEN regional_channel = 'SAD REGIONAL' THEN 'UNKNOWN'
	ELSE regional_channel
	END as region,
	branch as branch,
	subbranch as subbranch,
	cluster as cluster,
	CASE
	WHEN regional_channel = 'SUMBAGSEL' OR regional_channel = 'SUMBAGUT' OR regional_channel = 'SUMBAGTENG' THEN 'AREA 1'
	WHEN regional_channel = 'WESTERN JABOTABEK' OR regional_channel = 'EASTERN JABOTABEK' OR regional_channel = 'CENTRAL JABOTABEK' OR regional_channel = 'JABAR' THEN 'AREA 2'
	WHEN regional_channel = 'JATENG-DIY' OR regional_channel = 'JATIM' OR regional_channel = 'BALI NUSRA' THEN 'AREA 3'
	WHEN regional_channel = 'KALIMANTAN' OR regional_channel = 'SULAWESI' OR regional_channel = 'MALUKU DAN PAPUA' THEN 'AREA 4'
	ELSE 'UNKNOWN'
	END as area,
	node as node_type
FROM lacima_4g
WHERE regional_channel != ''