-- check_name: withinVisitDates
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: PROCEDURE_OCCURRENCE
-- cdm_field_name: PROCEDURE_DATE
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_within_visit_dates.sql
-- category: Conformance
-- subcategory: 
-- context: Verification
-- checkId: field_withinvisitdates_procedure_occurrence_procedure_date
-- thresholdValue: 5
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('withinVisitDates' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('PROCEDURE_OCCURRENCE' AS STRING) as cdm_table_name,
  CAST('PROCEDURE_DATE' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_within_visit_dates.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_withinvisitdates_procedure_occurrence_procedure_date' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  5 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  /*********
FIELD LEVEL check:
WITHIN_VISIT_DATES - find events that occur one week before the corresponding visit_start_date or one week after the corresponding visit_end_date
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = PROCEDURE_OCCURRENCE
cdmFieldName = PROCEDURE_DATE
**********/
SELECT num_violated_rows,
  CASE
    WHEN denominator.num_rows = 0 THEN 0 
    ELSE 1.0*num_violated_rows/denominator.num_rows
  END AS pct_violated_rows,
  denominator.num_rows AS num_denominator_rows
FROM (
	SELECT 
	  COUNT(violated_rows.violating_field) AS num_violated_rows
	FROM (
		/*violatedRowsBegin*/
		SELECT 
		  'PROCEDURE_OCCURRENCE.PROCEDURE_DATE' AS violating_field, 
		  cdmTable.*
    FROM {cdm_schema}.PROCEDURE_OCCURRENCE cdmTable
      JOIN {cdm_schema}.visit_occurrence vo
      ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
    WHERE cdmTable.PROCEDURE_DATE < dateadd(day, -7, vo.visit_start_date)
      OR cdmTable.PROCEDURE_DATE > dateadd(day, 7, vo.visit_end_date)
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.PROCEDURE_OCCURRENCE cdmTable
    JOIN {cdm_schema}.visit_occurrence vo
    ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
) denominator


) cte

