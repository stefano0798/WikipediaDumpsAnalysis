from pyspark import SparkContext
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import lit
from pyspark.sql import functions
from pyspark.sql import Window

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
# dataset_with_scores.show()

# convert seconds to seconds (integer format)
dataset_with_scores = dataset_with_scores.select(dataset_with_scores.page_id, dataset_with_scores.rev_id, functions.round(dataset_with_scores.timestamp).cast('integer').alias("timestamp"), functions.round(dataset_with_scores.current_timestamp).cast('integer').alias("current_timestamp"), dataset_with_scores.score)
dataset_with_scores
# dataset_with_scores.show()

# calculate score using these values...
high_score = 3 # points gained for recent edits ( < recent_limit ) (default: 3)
low_score = 1 # points gained for older edits ( between recent_limit and old_limit) (default: 1)
recent_limit = 1104541200 # number of seconds before an edit is considered new (default: 604800, one week)
old_limit = 1167613200 # number of seconds before an edit is still considerable (default: 2419200, one month)
# very important that recent_limit < old_limit 
# in this way, an edit gains three points if it is more recent than one week, while it gains one point if it is between one week and one month old. No points gained for ones older than one month
dataset_with_scores = dataset_with_scores.select(dataset_with_scores.page_id, dataset_with_scores.rev_id, dataset_with_scores.timestamp, dataset_with_scores.current_timestamp, functions.when(dataset_with_scores.current_timestamp - dataset_with_scores.timestamp <= recent_limit, high_score).when( ((dataset_with_scores.current_timestamp - dataset_with_scores.timestamp <= old_limit) & (dataset_with_scores.current_timestamp - dataset_with_scores.timestamp > recent_limit)), low_score).otherwise(0).alias("score"))
dataset_with_scores
# dataset_with_scores.show()

#rename for shorter typing...
data = dataset_with_scores

# save the timestamp of the first review to compute the general review/day rate
dataset = data.select('page_id', 'rev_id', 'timestamp', 'score', functions.min('timestamp').over(Window.partitionBy('page_id')).alias('first_edit'))

#count how many total reviews per page_id
total_reviews = dataset.select(dataset.page_id)
total_reviews = total_reviews.groupBy('page_id').count().withColumnRenamed("count", "review_number").withColumnRenamed("page_id", "page_id_new")
dataset = dataset.join(total_reviews, dataset.page_id == total_reviews.page_id_new, how = 'inner')

# delete duplicate join column
dataset = dataset.select('page_id', 'rev_id', 'timestamp', 'score', 'first_edit', 'review_number')

# count how many reviews in the last period ( < recent limit )
latest_reviews = dataset.filter(dataset.score == high_score)
latest_reviews = latest_reviews.withColumnRenamed("page_id", "page_id_new")
latest_reviews = latest_reviews.groupBy(latest_reviews.page_id_new).count().select(latest_reviews.page_id_new, functions.col('count').alias('recent_reviews'))
dataset = dataset.join(latest_reviews, dataset.page_id == latest_reviews.page_id_new, how = 'inner')


# delete duplicate column for id and remove useless columns
dataset = dataset.select('page_id', 'rev_id', 'timestamp', 'score', 'review_number', 'first_edit', 'recent_reviews')

dataset = dataset.groupBy("page_id", "first_edit", "recent_reviews", "review_number").sum("score").withColumnRenamed("sum(score)", "score")
# dataset.show()

dataset = dataset.withColumn("current_timestamp", functions.unix_timestamp(lit(functions.current_timestamp())))
dataset = dataset.withColumn("edit_per_day", (dataset.review_number / (((dataset.current_timestamp - dataset.first_edit)/(86400)))))
# dataset.show()
dataset = dataset.drop(dataset.current_timestamp)
# dataset.show()

# compute score as points/#reviews
dataset = dataset.withColumn("score", (dataset.score/dataset.review_number))

# compute recent reviews as percentage (#recent_reviews/#total_reviews)
dataset = dataset.withColumn("recent_reviews", (dataset.recent_reviews/dataset.review_number))

#dropping useless columns and printing final result
dataset = dataset.drop(dataset.first_edit)
dataset = dataset.drop(dataset.review_number)

dataset.show()
