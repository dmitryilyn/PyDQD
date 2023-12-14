from pydqd import get_check_list, execute_checks, generate_json_report #, get_metadata, get_check_results, get_results_overview

cdm_schema = "cdm_schema"
results_schema = cdm_schema
output_folder = "output"

check_list = get_check_list("queries_spark", cdm_schema)

# Initialize spark session here or use an existing one

execute_checks(spark, output_folder, check_list, results_schema)

#metadata = get_metadata(spark, cdm_schema)
#check_results = get_check_results(spark, results_schema)
#results_overview = get_results_overview(spark, results_schema)

generate_json_report(spark, cdm_schema, results_schema)
