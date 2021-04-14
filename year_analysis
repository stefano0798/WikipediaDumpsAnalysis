#Not used

#Year analysis

from pyspark import SparkContext
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from datetime import timedelta
from pyspark.sql.functions import col, udf,desc
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql import functions
from pyspark.sql.functions import dayofyear, year, month, count, mean
from pyspark.sql.functions import sum as sql_sum
from pyspark.sql.functions import broadcast, coalesce, when
from pyspark.sql import functions as F

import sys
reload(sys)
sys.setdefaultencoding('utf8')

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

#Command to access the file list
PATH ="/user/s2575760/project/data/enwiki-202012010-pages-meta-history/*"
df = spark.read.csv(PATH,header="true")

# group by date and page id - count rev_id - sort by count
df0 = df.select("page_id", "page_title", "rev_id", "text_size", "contributor_ip", "contributor_user_id", to_timestamp(df.timestamp, 'yyyy-MM-dd').alias('date'), "text_size")
df1 = df0.filter((df0.date >= '2008-01-01') & (df0.date <= '2009-12-31')).filter(~(df0.page_title.contains("Wikipedia:"))).filter(~(df0.page_title.contains("List of:"))).filter(~(df0.page_title.contains("User:"))).filter(~(df0.page_title.contains("Template:"))).filter(~(df0.page_title.contains("Template talk:"))).filter(~(df0.page_title.contains("Wikipedia talk:"))).filter(~(df0.page_title.contains("Portal:"))).filter(~(df0.page_title.contains("User talk:"))).filter(~(df0.page_title.contains("Talk:"))).filter(~(df0.page_title.contains("Category:"))).filter(~(df0.page_title.contains("File:"))).filter(~(df0.page_title.contains("Category talk:"))).filter(~(df0.page_title.contains("Portal talk:"))).filter(~(df0.page_title.contains("MediaWiki talk:"))).filter(~(df0.page_title.contains("File talk:"))).filter(~(df0.page_title.contains("Page talk:"))).filter(~(df0.page_title.contains("Help talk:"))).filter(~(df0.page_title.contains("List of"))).filter(~(df0.page_title.contains("Deaths in")))

# count of total edits
data_2008 = df1.filter((df0.date >= '2008-01-01') & (df0.date <= '2008-12-31')).groupBy("page_id", "page_title").count().withColumnRenamed("count", "total_rev_count").sort(desc("total_rev_count")).limit(10).withColumnRenamed("page_id", "p_id")
data_2009 = df1.filter((df0.date >= '2009-01-01') & (df0.date <= '2009-12-31')).groupBy("page_id", "page_title").count().withColumnRenamed("count", "total_rev_count").sort(desc("total_rev_count")).limit(10).withColumnRenamed("page_id", "p_id")

#Join data for each year
df_group_by_page = data_2008.union(data_2009)

# edit count per day
df_group_by_page_per_day = df1.groupBy("page_id", "date").agg(F.count("page_id").alias("day_rev_count"), F.sum("text_size").alias("total_text_size")).sort(desc("date"))

#get top 20 pages with most edits
df_top20pg_edits = df_group_by_page.join(df_group_by_page_per_day, df_group_by_page_per_day.page_id == df_group_by_page.p_id).sort("page_id", "date")

#get all the data in one file
result_top20pg = df_top20pg_edits.coalesce(1).cache()

#create csv
result_top20pg.write.format('csv').mode("overwrite").option("header","true").save('/user/s2475650/result_top10_2008_2009_result.csv')
