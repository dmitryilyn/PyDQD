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
  ,'plausibleAfterBirth' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of records with a date value in the CONDITION_END_DATETIME field of the CONDITION_OCCURRENCE table that occurs prior to birth.' as check_description
  ,'CONDITION_OCCURRENCE' as cdm_table_name
  ,'CONDITION_END_DATETIME' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_plausible_after_birth.sql' as sql_file
  ,'Plausibility' as category
  ,'Temporal' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_plausibleafterbirth_condition_occurrence_condition_end_datetime' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,1 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
PLAUSIBLE_AFTER_BIRTH
Checks that all events happen after birth (PLAUSIBLE_AFTER_BIRTH == Yes)
Birthdate is either birth_datetime or composed from year_of_birth, month_of_birth, day_of_birth (taking 1st month/1st day if missing).
Denominator is number of events with a non-null date.
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = CONDITION_OCCURRENCE
cdmFieldName = CONDITION_END_DATETIME
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
            'CONDITION_OCCURRENCE.CONDITION_END_DATETIME' AS violating_field, 
            cdmTable.*
        FROM {cdm_schema}.CONDITION_OCCURRENCE cdmTable
        JOIN {cdm_schema}.person p 
            ON cdmTable.person_id = p.person_id
        WHERE cdmTable.CONDITION_END_DATETIME IS NOT NULL AND 
            CAST(cdmTable.CONDITION_END_DATETIME AS DATE) < COALESCE(
                p.birth_datetime, 
                CAST(CONCAT(
                    p.year_of_birth,
                    COALESCE(
                        RIGHT('0' || CAST(p.month_of_birth AS VARCHAR), 2),
                        '01'
                    ),
                    COALESCE(
                        RIGHT('0' || CAST(p.day_of_birth AS VARCHAR), 2),
                        '01'
                    )
                ) AS DATE)
            )
        /*violatedRowsEnd*/
    ) violated_rows
) violated_row_count,
(
    SELECT 
        COUNT(*) AS num_rows
    FROM {cdm_schema}.CONDITION_OCCURRENCE cdmTable
    WHERE cdmTable.CONDITION_END_DATETIME IS NOT NULL
) denominator
) cte

