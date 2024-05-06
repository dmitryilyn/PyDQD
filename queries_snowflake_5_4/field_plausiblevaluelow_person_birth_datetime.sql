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
  ,'plausibleValueLow' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of records with a value in the BIRTH_DATETIME field of the PERSON table less than ''18500101''.' as check_description
  ,'PERSON' as cdm_table_name
  ,'BIRTH_DATETIME' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_plausible_value_low.sql' as sql_file
  ,'Plausibility' as category
  ,'Atemporal' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_plausiblevaluelow_person_birth_datetime' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,1 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
PLAUSIBLE_VALUE_LOW
get number of records and the proportion to total number of eligible records that fall below this threshold
Parameters used in this template:
schema = sqlgen
cdmTableName = PERSON
cdmFieldName = BIRTH_DATETIME
plausibleValueLow = '18500101'
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
            'PERSON.BIRTH_DATETIME' AS violating_field, 
            cdmTable.*
        FROM {cdm_schema}.PERSON cdmTable
            WHERE CAST(cdmTable.BIRTH_DATETIME AS DATE) < CAST('18500101' AS DATE)
        /*violatedRowsEnd*/
    ) violated_rows
) violated_row_count,
(
    SELECT 
        COUNT(*) AS num_rows
    FROM {cdm_schema}.PERSON cdmTable
    WHERE BIRTH_DATETIME IS NOT NULL
) denominator
) cte

