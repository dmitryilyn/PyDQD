import os
import re
from tqdm import tqdm


def get_check_list(directory, cdm_schema):
    check_list = []
    total_files = sum(1 for filename in os.listdir(directory) if filename.endswith(".sql"))

    for filename in tqdm(os.listdir(directory), desc="Processing SQL files", total=total_files, unit="file"):
        if filename.endswith(".sql"):
            file_path = os.path.join(directory, filename)
            with open(file_path, "r") as file:
                sql_content = file.read()

                check_name = re.search(r"-- check_name: (.+)", sql_content)
                check_level = re.search(r"-- check_level: (.+)", sql_content)
                check_description = re.search(r"-- checkDescription: (.+)", sql_content)
                cdm_table_name = re.search(r"-- cdm_table_name: (.+)", sql_content)
                cdm_field_name = re.search(r"-- cdm_field_name: (.+)", sql_content)
                concept_id = re.search(r"-- concept_id: (.+)", sql_content)
                unit_concept_id = re.search(r"-- unit_concept_id: (.+)", sql_content)
                sql_file = re.search(r"-- sql_file: (.+)", sql_content)
                category = re.search(r"-- category: (.+)", sql_content)
                subcategory = re.search(r"-- subcategory: (.+)", sql_content)
                context = re.search(r"-- context: (.+)", sql_content)
                check_id = re.search(r"-- checkId: (.+)", sql_content)
                threshold_value = re.search(r"-- thresholdValue: (.+)", sql_content)

                check = {
                    "check_name": check_name.group(1) if check_name else "",
                    "check_level": check_level.group(1) if check_level else "",
                    "check_description": check_description.group(1) if check_description else "",
                    "cdm_table_name": cdm_table_name.group(1) if cdm_table_name else "",
                    "cdm_field_name": cdm_field_name.group(1) if cdm_field_name else "",
                    "concept_id": concept_id.group(1) if concept_id else "",
                    "unit_concept_id": unit_concept_id.group(1) if unit_concept_id else "",
                    "sql_file": sql_file.group(1) if sql_file else "",
                    "category": category.group(1) if category else "",
                    "subcategory": subcategory.group(1) if subcategory else "",
                    "context": context.group(1) if context else "",
                    "check_id": check_id.group(1) if check_id else "",
                    "threshold_value": threshold_value.group(1) if threshold_value else "",
                    "sql": re.sub(r"-- .+\n", "", re.sub(r"\{cdm_schema\}", cdm_schema, sql_content))
                }

                check_list.append(check)

    return check_list


def execute_checks(spark, output_folder, check_list, results_schema, results_table="pydqd_results", batch_size=4):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    existing_check_ids = []
    try:
        existing_check_ids_df = spark.sql(f"SELECT DISTINCT checkid FROM {results_schema}.{results_table}")
        existing_check_ids = [row.checkid for row in existing_check_ids_df.collect()]
    except Exception as select_error:
        print(f"Error selecting existing checkIds: {str(select_error)}")
    
    if not existing_check_ids:
        spark.sql(f"DROP TABLE IF EXISTS {results_schema}.{results_table} PURGE")
        ddl = f"""
            CREATE TABLE {results_schema}.{results_table} (
                num_violated_rows INT,
                pct_violated_rows FLOAT,
                num_denominator_rows INT,
                execution_time FLOAT,
                query_text STRING,
                check_name STRING,
                check_level STRING,
                check_description STRING,
                cdm_table_name STRING,
                cdm_field_name STRING,
                concept_id INT,
                unit_concept_id INT,
                sql_file STRING,
                category STRING,
                subcategory STRING,
                context STRING,
                warning STRING,
                error STRING,
                checkid STRING,
                is_error INT,
                not_applicable INT,
                failed INT,
                passed INT,
                not_applicable_reason STRING,
                threshold_value INTEGER,
                notes_value STRING
            ) USING PARQUET
            """
        spark.sql(ddl)

    full_num_checks = len(check_list)
    check_list = [check for check in check_list if check["check_id"] not in existing_check_ids]
    num_checks = len(check_list)
    print(f"Executing {num_checks} ({full_num_checks-num_checks} are done)...")
    
    for i in tqdm(range(0, num_checks, batch_size), desc="Processing checks", unit=f"batch of {batch_size}"):
        batch = check_list[i:i + batch_size]
        union_sql = "\nUNION ALL\n".join(check["sql"] for check in batch)
        insert_sql = f"INSERT INTO {results_schema}.{results_table}\n{union_sql};"
        try:
            spark.sql(insert_sql)
        except Exception as insert_error:
            log_filename = f"{output_folder}/error_batch_{i+1}.txt"
            with open(log_filename, 'w') as log_file:
                log_file.write(f"Error inserting data: {str(insert_error)}")
            print(f"Error inserting data: {str(insert_error)}")


def get_metadata(spark, cdm_schema):
    metadata_df = spark.sql(f"select * from {cdm_schema}.cdm_source")
    metadata_dict = {col.upper(): value for col, value in metadata_df.first().asDict().items() if col.upper() not in ["RULE_ID", "LOAD_TABLE_ID", "LOAD_ROW_ID"]}
    metadata_dict["DQD_VERSION"] = "2.0.0"
    
    return metadata_dict


def get_check_results(spark, results_schema, results_table="pydqd_results"):
    results_df = spark.sql(f"select * from {results_schema}.{results_table}")
    check_results = []
    for row in tqdm(results_df.collect()):
        check_result = {}
        for col, value in row.asDict().items():
            if col.lower() == "checkid":
                check_result["checkId"] = value
            else:
                check_result[col.upper()] = value
        check_results.append(check_result)
    
    return check_results


def get_results_overview(spark, results_schema, results_table="pydqd_results"):
    sql_query = f'''
    SELECT 
        COUNT(*) AS countTotal,
        SUM(passed = 1) AS countPassed,
        SUM(is_error = 1) AS countErrorFailed,
        SUM(failed = 1) AS countThresholdFailed,
        SUM((is_error = 1 OR failed = 1)) AS countOverallFailed,
        (SUM(passed = 1) / COUNT(*)) * 100 AS percentPassed,
        (SUM((is_error = 1 OR failed = 1)) / COUNT(*)) * 100 AS percentFailed,
        SUM(category = 'Plausibility') AS countTotalPlausibility,
        SUM(category = 'Conformance') AS countTotalConformance,
        SUM(category = 'Completeness') AS countTotalCompleteness,
        SUM((is_error = 1 OR failed = 1) AND category = 'Plausibility') AS countFailedPlausibility,
        SUM((is_error = 1 OR failed = 1) AND category = 'Conformance') AS countFailedConformance,
        SUM((is_error = 1 OR failed = 1) AND category = 'Completeness') AS countFailedCompleteness,
        SUM(passed = 1 AND category = 'Plausibility') AS countPassedPlausibility,
        SUM(passed = 1 AND category = 'Conformance') AS countPassedConformance,
        SUM(passed = 1 AND category = 'Completeness') AS countPassedCompleteness
    FROM {results_schema}.{results_table}
    '''
    results_df = spark.sql(sql_query)
    results_overview = {col.lower(): value for col, value in results_df.first().asDict().items()}

    field_names = [
        "countTotal",
        "countPassed",
        "countErrorFailed",
        "countThresholdFailed",
        "countOverallFailed",
        "percentPassed",
        "percentFailed",
        "countTotalPlausibility",
        "countTotalConformance",
        "countTotalCompleteness",
        "countFailedPlausibility",
        "countFailedConformance",
        "countFailedCompleteness",
        "countPassedPlausibility",
        "countPassedConformance",
        "countPassedCompleteness"
    ]
    for field_name in field_names:
        results_overview[field_name] = [results_overview.pop(field_name.lower())]
    
    return results_overview
