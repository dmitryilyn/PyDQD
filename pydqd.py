import os
import re
import argparse
from tqdm import tqdm
from datetime import datetime
from pyspark.sql import SparkSession


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


def execute_checks(output_folder, check_list, results_schema, results_table="pydqd_results", batch_size=4):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    spark = SparkSession.builder.appName("PyDQD").getOrCreate()
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
        ) STORED AS PARQUET
        """
    spark.sql(ddl)

    num_checks = len(check_list)
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


def main():
    parser = argparse.ArgumentParser(description="Data Quality Dashboard on pyspark.")
    parser.add_argument("--conf", help="Path to spark.conf (optional).")
    parser.add_argument("--cdm", required=True, help="Name of CDM schema.")
    parser.add_argument("--results_schema", default="", help="Results schema.")
    parser.add_argument("--results_table", default="pydqd_results", help="Results table in the results schema.")
    parser.add_argument("--output", default=f"output_{datetime.now().strftime('%y%m%d-%H%M%S')}", help="Path to the output folder.")
    
    args = parser.parse_args()

    if not (args.conf or args.cdm):
        parser.print_help()
        return

    sql_folder = "queries_spark"
    check_list = get_check_list(sql_folder, args.cdm)
    
    results_schema = args.results_schema
    if not results_schema:
        results_schema = args.cdm
    execute_checks(args.output, check_list, results_schema, args.results_table)


if __name__ == "__main__":
    main()
