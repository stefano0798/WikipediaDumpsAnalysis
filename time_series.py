# Time series page_revisions per month in 2020

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
from pyspark.sql.functions import broadcast

import sys
reload(sys)
sys.setdefaultencoding('utf8')

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

#Command to access the file list: hdfs dfs -ls /user/s2575760/project/data/enwiki-202012010-pages-meta-history/
PATH ="/user/s2575760/project/data/enwiki-202012010-pages-meta-history/*"
#PATH ="/user/s2575760/project/data/enwiki-202012010-pages-meta-history/enwiki-20201201-pages-meta-history8.xml-p2727760p2773697_l_wikifields.csv"
df = spark.read.csv(PATH,header="true")

#df0 = df.select("page_id", "page_title", "page_ns", "rev_id", "parent_revid", "model", "format", "text_size", "sha1", "contributor_ip", "contributor_user_id", "contributor_username", to_timestamp(df.timestamp, 'yyyy-MM-dd').alias('date'))
df0 = df.select("page_id", "page_title", "rev_id", "text_size", "contributor_ip", "contributor_user_id", to_timestamp(df.timestamp, 'yyyy-MM-dd').alias('date'), "text_size")
df1 = df0.filter((df0.date >= '2010-01-01') & (df0.date <= '2019-12-31'))
df2 = df1.groupBy("page_id", "page_title",  "rev_id", "text_size", dayofyear("date").alias("day"), month("date").alias("month"), year("date").alias("year")).agg(count("rev_id").alias("count_revisions")).sort(desc("count_revisions"))

df2.createOrReplaceTempView("wiki_data")
df3 = spark.sql("select page_title, page_id, sum(count_revisions) as count_revisions from wiki_data where page_title not like 'Wikipedia%' group by 1, 2 order by count_revisions desc limit 20")

print(df2.count()) #from 2000 to 2019: 317,746,376   - from 2010 to 2019: 158,518,977
print(df3.count()) #20

df4 = df2.join(broadcast(df3), df2.page_id == df3.page_id, "leftsemi")
df5 = df4.groupBy("page_id", "page_title", "month", "year").agg(sql_sum(df4.count_revisions).alias("count_revisions"), sql_sum(df4.text_size).alias("text_size"))

s_2010_2019 = df5.filter((df5.year >= '2010') & (df5.year <= '2019'))

#print(series_2010_2019.show())
#print(series_2010_2019.count())

series_2010_2019 = s_2010_2019.coalesce(1)

#print(series_2010_2019.count())

series_2010_2019.write.format('csv').mode("overwrite").option("header","true").save('/user/s2475650/series_2000_2019_final.csv')
df3.write.format('csv').mode("overwrite").option("header","true").save('/user/s2475650/series_2000_2019_top20.csv')
