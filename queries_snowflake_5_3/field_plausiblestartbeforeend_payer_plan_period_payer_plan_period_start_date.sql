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
  ,'plausibleStartBeforeEnd' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of records with a value in the PAYER_PLAN_PERIOD_START_DATE field of the PAYER_PLAN_PERIOD that occurs after the date in the PAYER_PLAN_PERIOD_END_DATE.' as check_description
  ,'PAYER_PLAN_PERIOD' as cdm_table_name
  ,'PAYER_PLAN_PERIOD_START_DATE' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_plausible_start_before_end.sql' as sql_file
  ,'Plausibility' as category
  ,'Temporal' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_plausiblestartbeforeend_payer_plan_period_payer_plan_period_start_date' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,1 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
PLAUSIBLE_START_BEFORE_END
Checks that all start dates are before their corresponding end dates (PLAUSIBLE_START_BEFORE_END == Yes).
PAYER_PLAN_PERIOD_START_DATE is the start date and PAYER_PLAN_PERIOD_END_DATE is the end date.
Parameters used in this template:
schema = sqlgen
cdmTableName = PAYER_PLAN_PERIOD
cdmFieldName = PAYER_PLAN_PERIOD_START_DATE
plausibleStartBeforeEndFieldName = PAYER_PLAN_PERIOD_END_DATE
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
            'PAYER_PLAN_PERIOD.PAYER_PLAN_PERIOD_START_DATE' AS violating_field, 
            cdmTable.*
        FROM {cdm_schema}.PAYER_PLAN_PERIOD cdmTable
        WHERE cdmTable.PAYER_PLAN_PERIOD_START_DATE IS NOT NULL 
            AND cdmTable.PAYER_PLAN_PERIOD_END_DATE IS NOT NULL 
            AND CAST(cdmTable.PAYER_PLAN_PERIOD_START_DATE AS DATE) > CAST(cdmTable.PAYER_PLAN_PERIOD_END_DATE AS DATE)
        /*violatedRowsEnd*/
    ) violated_rows
) violated_row_count,
(
    SELECT 
        COUNT(*) AS num_rows
    FROM {cdm_schema}.PAYER_PLAN_PERIOD cdmTable
    WHERE cdmTable.PAYER_PLAN_PERIOD_START_DATE IS NOT NULL 
        AND cdmTable.PAYER_PLAN_PERIOD_END_DATE IS NOT NULL
) denominator
) cte

