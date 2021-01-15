from pyspark import SparkContext
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import lit
from pyspark.sql import functions

sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

df = spark.read.csv("/user/s2575760/project/data/sample/preprocesed/wiki_fields_enwiki-20201201-pages-meta-history2.xml-p151386p151573.csv", header="true")

data = df.select(df.page_id, df.rev_id, df.timestamp)


# data = data.groupBy(data.page_id)
# data = data.count()
# data = data.orderBy("count", ascending=False) 
# data.show(5)

# data = df.select(df.page_title, df.rev_id)
# data = data.groupBy(data.page_title)
# data = data.count()
# data = data.orderBy("count", ascending=False)
# data.show(5)

dataset = data.select(data.page_id, data.rev_id, to_timestamp(data.timestamp).alias('timestamp'))

# add current timestamp to compare time difference
dataset_with_scores = dataset.withColumn("current_timestamp", lit(functions.current_timestamp()))
# initialize score column to 0
dataset_with_scores = dataset_with_scores.withColumn("score", lit(0))

# transform timestamps to integer in order to retrieve difference (in seconds)
dataset_with_scores = dataset_with_scores.select(dataset_with_scores.page_id, dataset_with_scores.rev_id, functions.unix_timestamp(dataset_with_scores.timestamp).alias("timestamp"), functions.unix_timestamp(dataset_with_scores.current_timestamp).alias("current_timestamp"), dataset_with_scores.score)
dataset_with_scores
dataset_with_scores.show()

# convert seconds to seconds (integer format)
dataset_with_scores = dataset_with_scores.select(dataset_with_scores.page_id, dataset_with_scores.rev_id, functions.round(dataset_with_scores.timestamp).cast('integer').alias("timestamp"), functions.round(dataset_with_scores.current_timestamp).cast('integer').alias("current_timestamp"), dataset_with_scores.score)
dataset_with_scores
dataset_with_scores.show()

# calculate score using these values...
high_score = 3 # points gained for recent edits ( < recent_limit ) (default: 3)
low_score = 1 # points gained for older edits ( between recent_limit and old_limit) (default: 1)
recent_limit = 604800 # number of seconds before an edit is considered new (default: 604800, one week)
old_limit = 2419200 # number of seconds before an edit is still considerable (default: 2419200, one month)
# in this way, an edit gains three points if it is more recent than one week, while it gains one point if it is between one week and one month old. No points gained for ones older than one month
dataset_with_scores = dataset_with_scores.select(dataset_with_scores.page_id, dataset_with_scores.rev_id, dataset_with_scores.timestamp, dataset_with_scores.current_timestamp, functions.when(dataset_with_scores.current_timestamp - dataset_with_scores.timestamp <= recent_limit, high_score).when( ((dataset_with_scores.current_timestamp - dataset_with_scores.timestamp <= old_limit) & (dataset_with_scores.current_timestamp - dataset_with_scores.timestamp > recent_limit)), low_score).otherwise(0).alias("score"))
dataset_with_scores
dataset_with_scores.show()

#rename for shorter typing...
data = dataset_with_scores

grouped = data.groupBy("page_id").min("timestamp").show()
