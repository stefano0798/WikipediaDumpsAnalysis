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

#add current timestamp to compare time difference
dataset_with_scores = dataset.withColumn("current_timestamp", lit(functions.current_timestamp()))
#initialize score column to 0
dataset_with_scores = dataset_with_scores.withColumn("score", lit(0))

#transform timestamps to integer in order to retrieve difference (in seconds)
dataset_with_scores = dataset_with_scores.select(dataset_with_scores.page_id, dataset_with_scores.rev_id, functions.unix_timestamp(dataset_with_scores.timestamp).alias("timestamp"), functions.unix_timestamp(dataset_with_scores.current_timestamp).alias("current_timestamp"), dataset_with_scores.score)
dataset_with_scores
dataset_with_scores.show()

#convert seconds to seconds (integer format)
dataset_with_scores = dataset_with_scores.select(dataset_with_scores.page_id, dataset_with_scores.rev_id, functions.round(dataset_with_scores.timestamp).cast('integer').alias("timestamp"), functions.round(dataset_with_scores.current_timestamp).cast('integer').alias("current_timestamp"), dataset_with_scores.score)
dataset_with_scores
dataset_with_scores.show()

dataset_with_scores = dataset_with_scores.select(dataset_with_scores.page_id, dataset_with_scores.rev_id, dataset_with_scores.timestamp, dataset_with_scores.current_timestamp, functions.when(dataset_with_scores.current_timestamp - dataset_with_scores.timestamp < 604799, 3).when( ((dataset_with_scores.current_timestamp - dataset_with_scores.timestamp < 2419200) & (dataset_with_scores.current_timestamp - dataset_with_scores.timestamp > 604800)), 1).otherwise(0).alias("score"))
dataset_with_scores
dataset_with_scores.show()

