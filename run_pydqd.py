from pydqd import get_check_list, execute_checks

cdm_schema = "cdm_schema"
results_schema = cdm_schema
output_folder = "output"

check_list = get_check_list("queries_spark", cdm_schema)

execute_checks(spark, output_folder, check_list, results_schema)