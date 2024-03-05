-- =============================================
-- FileName: usp_ai_hierarchy.sql
-- Description:
--   This Procedure create hierachy table with current records
--
-- =============================================
ALTER PROCEDURE [SCHEMA_NAME].[usp_ai_hierarchy]
AS


SET NOCOUNT ON;

	TRUNCATE TABLE [DATABASE_NAME].[SCHEMA_NAME].[temp_hier_blpu_parent_child] ;

	INSERT INTO  [DATABASE_NAME].[SCHEMA_NAME].[temp_hier_blpu_parent_child]
	SELECT address_entry_id, detail_valid_from_date, detail_valid_to_date, parent_address_entry_id, address_entry_id as primary_address_entry_id, NULL as secondary_address_entry_id,
	uprn, parent_uprn, uprn as primary_uprn, NULL as secondary_uprn, 1 as layers, 1 as this_layer
	FROM rdmf_uat.rdmf.blpu m
	WHERE  current_record_flag=1 AND parent_address_entry_id IS NULL
	UNION
	SELECT address_entry_id, detail_valid_from_date, detail_valid_to_date, parent_address_entry_id, NULL as primary_address_entry_id, NULL as secondary_address_entry_id,
	uprn, parent_uprn, NULL as primary_uprn, NULL as secondary_uprn, 0 as layers, 0 as this_layer
	FROM rdmf_uat.rdmf.blpu m
	WHERE current_record_flag=1 AND  parent_address_entry_id IS NOT NULL;

	TRUNCATE TABLE [DATABASE_NAME].[SCHEMA_NAME].[temp_hier_blpu_uprn_no_parent];

	INSERT INTO [DATABASE_NAME].[SCHEMA_NAME].[temp_hier_blpu_uprn_no_parent]
	SELECT m.address_entry_id, m.detail_valid_from_date, m.detail_valid_to_date, m.parent_address_entry_id, NULL as primary_address_entry_id, NULL as secondary_address_entry_id,
	m.uprn, m.parent_uprn, NULL as primary_uprn, NULL as secondary_uprn, 1 as layers, 1 as this_layer
	FROM
	(SELECT address_entry_id, detail_valid_from_date, detail_valid_to_date, parent_address_entry_id, NULL as primary_address_entry_id, NULL as secondary_address_entry_id,
	uprn, parent_uprn, NULL as primary_uprn, NULL as secondary_uprn, 0 as layers, 0 as this_layer
	FROM rdmf_uat.rdmf.blpu
	WHERE current_record_flag=1 AND  parent_address_entry_id IS NOT NULL) m
	LEFT JOIN rdmf_uat.rdmf.blpu pm ON pm.current_record_flag=1 AND m.parent_uprn= pm.uprn
	WHERE pm.uprn is NULL

	TRUNCATE TABLE [DATABASE_NAME].[SCHEMA_NAME].[temp_hierarchy];

	WITH hier_tbl AS (

		SELECT address_entry_id, hierarchy_valid_from_date, hierarchy_valid_to_date, parent_address_entry_id, address_entry_id as primary_address_entry_id, secondary_address_entry_id,
				uprn, parent_uprn, uprn as primary_uprn, secondary_uprn, layers, this_layer
				FROM [DATABASE_NAME].[SCHEMA_NAME].[temp_hier_blpu_parent_child]
				WHERE parent_address_entry_id IS NULL
		UNION ALL
		SELECT address_entry_id, hierarchy_valid_from_date, hierarchy_valid_to_date, parent_address_entry_id, address_entry_id as primary_address_entry_id, secondary_address_entry_id,
				uprn, parent_uprn, uprn as primary_uprn, secondary_uprn, layers, this_layer
				FROM [DATABASE_NAME].[SCHEMA_NAME].[temp_hier_blpu_uprn_no_parent]
		UNION ALL
		SELECT  m.address_entry_id, a.hierarchy_valid_from_date, m.hierarchy_valid_to_date, m.parent_address_entry_id, a.primary_address_entry_id,
			CASE
			WHEN (a.this_layer=1) THEN m.address_entry_id
			ELSE a.secondary_address_entry_id
			END as secondary_address_entry_id,
				m.uprn, m.parent_uprn, a.primary_uprn,
			CASE
			WHEN (a.this_layer=1) THEN m.uprn
			ELSE a.secondary_uprn
			END as secondary_uprn,
		a.layers + 1 as layers,
		a.layers + 1 as this_layer
		FROM [DATABASE_NAME].[SCHEMA_NAME].[temp_hier_blpu_parent_child] m
		JOIN hier_tbl a
		ON m.parent_address_entry_id = a.address_entry_id
	)

	INSERT INTO [DATABASE_NAME].[SCHEMA_NAME].[temp_hierarchy]
	SELECT address_entry_id, hierarchy_valid_from_date, hierarchy_valid_to_date, parent_address_entry_id, primary_address_entry_id, secondary_address_entry_id,
	uprn, parent_uprn, primary_uprn, secondary_uprn, layers, this_layer
	FROM hier_tbl;


	UPDATE hr
	SET layers = hr2.max_layer
	FROM [DATABASE_NAME].[SCHEMA_NAME].[temp_hierarchy] hr
	INNER JOIN (SELECT primary_address_entry_id,MAX(this_layer) OVER (PARTITION BY primary_address_entry_id) AS max_layer
		FROM [DATABASE_NAME].[SCHEMA_NAME].[temp_hierarchy] ) hr2
	ON hr.primary_address_entry_id = hr2.primary_address_entry_id ;


	UPDATE tbh
	SET secondary_address_entry_id=(
	CASE
		WHEN (se2.primary_uprn IS NULL) THEN tbh.secondary_address_entry_id
		ELSE se2.secondary_address_entry_id
	END ),
	secondary_uprn= (CASE
		WHEN (se2.primary_uprn IS NULL) THEN tbh.secondary_uprn
		ELSE se2.secondary_uprn
	END)
	FROM [DATABASE_NAME].[SCHEMA_NAME].[temp_hierarchy] tbh
	LEFT JOIN (SELECT se.primary_uprn, tbh.secondary_uprn, tbh.secondary_address_entry_id FROM [DATABASE_NAME].[SCHEMA_NAME].[temp_hierarchy] tbh
	INNER JOIN (
		SELECT primary_uprn, this_layer FROM [DATABASE_NAME].[SCHEMA_NAME].[temp_hierarchy] WHERE this_layer=2
		GROUP BY primary_uprn, this_layer
		HAVING count(*)=1) se ON tbh.primary_uprn=se.primary_uprn AND tbh.this_layer=se.this_layer) se2
				ON tbh.primary_uprn=se2.primary_uprn AND tbh.this_layer=1;


	TRUNCATE TABLE [rdmf_uat].[rdmf].[hierarchy];

 	INSERT INTO [rdmf_uat].[rdmf].[hierarchy]
	SELECT address_entry_id, hierarchy_valid_from_date, hierarchy_valid_to_date, uprn, this_layer, layers, parent_uprn,
	parent_address_entry_id, primary_uprn, primary_address_entry_id, secondary_uprn, secondary_address_entry_id
	FROM [DATABASE_NAME].[SCHEMA_NAME].[temp_hierarchy];