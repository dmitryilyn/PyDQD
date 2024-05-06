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
  ,'plausibleBeforeDeath' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of records with a date value in the VISIT_DETAIL_END_DATE field of the VISIT_DETAIL table that occurs after death.' as check_description
  ,'VISIT_DETAIL' as cdm_table_name
  ,'VISIT_DETAIL_END_DATE' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_plausible_before_death.sql' as sql_file
  ,'Plausibility' as category
  ,'Temporal' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_plausiblebeforedeath_visit_detail_visit_detail_end_date' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,1 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
PLAUSIBLE_BEFORE_DEATH
Checks for events that occur more than 60 days after death (PLAUSIBLE_BEFORE_DEATH == Yes).
Denominator is number of events with a non-null date, of persons who died.
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = VISIT_DETAIL
cdmFieldName = VISIT_DETAIL_END_DATE
**********/
SELECT 
    num_violated_rows, 
    CASE 
        WHEN denominator.num_rows = 0 THEN 0 
        ELSE 1.0*num_violated_rows/denominator.num_rows 
    END AS pct_violated_rows, 
    denominator.num_rows AS num_denominator_rows
FROM
(
    SELECT 
        COUNT(violated_rows.violating_field) AS num_violated_rows
    FROM
    (
        /*violatedRowsBegin*/
        SELECT 
            'VISIT_DETAIL.VISIT_DETAIL_END_DATE' AS violating_field, 
            cdmTable.*
        FROM {cdm_schema}.VISIT_DETAIL cdmTable
        JOIN {cdm_schema}.death de 
            ON cdmTable.person_id = de.person_id
        WHERE cdmTable.VISIT_DETAIL_END_DATE IS NOT NULL 
            AND CAST(cdmTable.VISIT_DETAIL_END_DATE AS DATE) > DATEADD(day, 60, de.death_date)
        /*violatedRowsEnd*/
    ) violated_rows
) violated_row_count,
(
    SELECT 
        COUNT(*) AS num_rows
    FROM {cdm_schema}.VISIT_DETAIL cdmTable
    JOIN {cdm_schema}.death
        ON death.person_id = cdmTable.person_id
    WHERE cdmTable.VISIT_DETAIL_END_DATE IS NOT NULL
) denominator
) cte

