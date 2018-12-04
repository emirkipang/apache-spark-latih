SELECT
    no,
	CASE
        WHEN regional = 'BALI NUSRA' THEN 'BALINUSRA'
        WHEN regional = 'JATENG-DIY' THEN 'JATENG'
        WHEN regional = 'MALUKU DAN PAPUA' THEN 'PUMA'
        WHEN regional = 'SAD REGIONAL' THEN 'UNKNOWN'
        ELSE regional
	END as regional,
	branch as branch,
	subbranch as subbranch,
	cluster as cluster,
	CASE
        WHEN regional = 'SUMBAGSEL' OR regional = 'SUMBAGUT' OR regional = 'SUMBAGTENG' THEN 'AREA 1'
        WHEN regional = 'WESTERN JABOTABEK' OR regional = 'EASTERN JABOTABEK' OR regional = 'CENTRAL JABOTABEK' OR regional = 'JABAR' THEN 'AREA 2'
        WHEN regional = 'JATENG-DIY' OR regional = 'JATIM' OR regional = 'BALI NUSRA' THEN 'AREA 3'
        WHEN regional = 'KALIMANTAN' OR regional = 'SULAWESI' OR regional = 'MALUKU DAN PAPUA' THEN 'AREA 4'
        ELSE 'UNKNOWN'
	END as area,
	leadadname,
	leadadmsisdn,
	leadadcode
FROM list_ad