/*********
SQL to create query for insertion into results table. These may be unioned together prior to insert.
Note that this does not include information about SQL errors or performance.
**********/
SELECT 
  cte.num_violated_rows
  ,cte.pct_violated_rows
  ,cte.num_denominator_rows
  ,'' as execution_time
  ,'' as query_text
  ,'withinVisitDates' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
  ,'NOTE' as cdm_table_name
  ,'NOTE_DATE' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_within_visit_dates.sql' as sql_file
  ,'Conformance' as category
  ,'NA' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_withinvisitdates_note_note_date' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,5 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
FIELD LEVEL check:
WITHIN_VISIT_DATES - find events that occur one week before the corresponding visit_start_date or one week after the corresponding visit_end_date
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = NOTE
cdmFieldName = NOTE_DATE
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
		  'NOTE.NOTE_DATE' AS violating_field, 
		  cdmTable.*
    FROM {cdm_schema}.NOTE cdmTable
      JOIN {cdm_schema}.visit_occurrence vo
      ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
    WHERE cdmTable.NOTE_DATE < dateadd(day, -7, vo.visit_start_date)
      OR cdmTable.NOTE_DATE > dateadd(day, 7, vo.visit_end_date)
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.NOTE cdmTable
    JOIN {cdm_schema}.visit_occurrence vo
    ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
) denominator
) cte

