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
from pyspark.sql.functions import broadcast, coalesce, when

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
df0 = df.select("page_id", "page_title", "rev_id", "text_size", to_timestamp(df.timestamp, 'yyyy-MM-dd').alias('date'), "text_size")
df1 = df0.filter((df0.date >= '2010-01-01') & (df0.date <= '2019-12-31')).filter(~(df0.page_title.contains("Wikipedia:"))).filter(~(df0.page_title.contains("List of:"))).filter(~(df0.page_title.contains("User:"))).filter(~(df0.page_title.contains("Template:"))).filter(~(df0.page_title.contains("Template talk:"))).filter(~(df0.page_title.contains("Wikipedia talk:"))).filter(~(df0.page_title.contains("Portal:"))).filter(~(df0.page_title.contains("User talk:"))).filter(~(df0.page_title.contains("Talk:"))).filter(~(df0.page_title.contains("Category:")))
df2 = df1.groupBy("page_id", "page_title",  "rev_id", "text_size", "date", dayofyear("date").alias("day"), month("date").alias("month"), year("date").alias("year")).agg(count("rev_id").alias("count_revisions")).sort(desc("count_revisions"))

df2.createOrReplaceTempView("wiki_data")
df3 = spark.sql("select page_title, page_id, sum(count_revisions) as count_revisions from wiki_data where page_title not like 'Wikipedia%' and page_title not like 'List of%' and page_title  not like 'User:%' and page_title not like 'Template:%' and page_title not like 'Template talk:%' group by 1, 2 order by count_revisions desc limit 20").cache()

print(df2.count()) #from 2000 to 2019: 317,746,376   - from 2010 to 2019: 158,518,977
print(df3.count()) #20

spark.conf.set("spark.sql.broadcastTimeout", 500)
df4 = df2.join(broadcast(df3), df2.page_id == df3.page_id, "leftsemi")
df5 = df4.groupBy("page_id", "page_title", "date", "day", "month", "year").agg(sql_sum(df4.count_revisions).alias("count_revisions"), sql_sum(df4.text_size).alias("text_size"))

s_2010_2019 = df5.filter((df5.year >= '2010') & (df5.year <= '2019'))

#print(series_2010_2019.show())
#print(series_2010_2019.count())

series_2010_2019 = s_2010_2019.coalesce(1).cache()

#series_2010_2019.show()

print(series_2010_2019.show())
print(series_2010_2019.count())

series_2010_2019.write.format('csv').mode("overwrite").option("header","true").save('/user/s2475650/series_2010_2019_finalfile.csv')
df3.write.format('csv').mode("overwrite").option("header","true").save('/user/s2475650/series_2010_2019_finalfile_top20.csv')



#Transfer files from computer to cluster (usingbash)
#scp -r  /c/Users/Sarah/Desktop/MBD_project/time_series_grouped_day.py s2475650@ctit011.ewi.utwente.nl:/home/s2475650

#Transfer from HDFS to cluster
#hadoop fs -copyToLocal /user/s2475650/series_2010_2019_finalfile.csv /home/s2475650
#hadoop fs -copyToLocal /user/s2475650/series_2010_2019_finalfile_top20.csv /home/s2475650

#(Use on Bash) Trnsfer from cluster to local pc
#scp -r  s2475650@ctit011.ewi.utwente.nl:/home/s2475650/series_2010_2019_finalfile.csv /c/Users/Sarah/Desktop/MBD_project
#scp -r  s2475650@ctit011.ewi.utwente.nl:/home/s2475650/series_2010_2019_finalfile_top20.csv /c/Users/Sarah/Desktop/MBD_project

#execute Spark
#time spark-submit avg_textsize_user.py 2> /dev/null
#time spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=60 time_series_grouped_day.py
