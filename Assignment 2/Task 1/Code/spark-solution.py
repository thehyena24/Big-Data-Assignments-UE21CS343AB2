import sys
from pyspark.sql import SparkSession   
from pyspark.sql.functions import desc, collect_list, col

result = tuple()

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

csv_2012_path = sys.argv[1]
csv_2013_path = sys.argv[2]
csv_2014_path = sys.argv[3]
cases_state_key_path = sys.argv[4]
judges_clean_path = sys.argv[5]
judge_case_merge_key_path = sys.argv[6]
acts_section_path = sys.argv[7]
output_path = sys.argv[8]

csv_2012 = spark.read.csv(csv_2012_path, header = True, inferSchema = True)
csv_2013 = spark.read.csv(csv_2013_path, header = True, inferSchema = True)
csv_2014 = spark.read.csv(csv_2014_path, header = True, inferSchema = True)
cases_state_key = spark.read.csv(cases_state_key_path, header = True, inferSchema = True)

cumulative_csv = (csv_2012.union(csv_2013)).union(csv_2014)
cases_state_key = cases_state_key.filter((cases_state_key["year"]>=2012) & (cases_state_key["year"]<=2014))

cumulative_csv = cumulative_csv.join(cases_state_key, ["state_code"])

states_count = cumulative_csv.groupBy('state_name').count()
states_count = states_count.orderBy(desc("count")).limit(10)

states_list = [x["state_name"] for x in states_count.collect()]

csv_2012 = spark.read.csv(csv_2012_path, header = True, inferSchema = True)
csv_2013 = spark.read.csv(csv_2013_path, header = True, inferSchema = True)
csv_2014 = spark.read.csv(csv_2014_path, header = True, inferSchema = True)

cumulative_csv = csv_2012.union(csv_2013).union(csv_2014)
judges_clean = spark.read.csv(judges_clean_path, header = True, inferSchema = True)
judge_case_merge_key = spark.read.csv(judge_case_merge_key_path, header = True, inferSchema = True)
acts_section = spark.read.csv(acts_section_path, header = True, inferSchema = True)

joined_df = cumulative_csv.join(acts_section, "ddl_case_id")
joined_df = joined_df.filter(col("criminal") == 1)
joined_df = joined_df.join(judge_case_merge_key, "ddl_case_id")
joined_df = joined_df.filter(col("ddl_decision_judge_id").isNotNull())

current_csv = cumulative_csv.join(judge_case_merge_key, ["ddl_case_id"], 'inner')
current_csv = current_csv.join(acts_section, ["ddl_case_id"], 'inner')

current_csv = current_csv.filter(current_csv.criminal == 1)
judges_count = current_csv.groupBy('ddl_decision_judge_id').count()
judges_count = judges_count.filter(judges_count.ddl_decision_judge_id.isNotNull())
judges_count = judges_count.orderBy(desc("count")).limit(1)

judge = [x["ddl_decision_judge_id"] for x in judges_count.collect()]

result = (states_list, int(judge[0]))

with open(output_path, 'w') as f:
    f.write(str(result))