# Time series page_revisions per month in 2019

from pyspark import SparkContext
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from datetime import timedelta
from pyspark.sql.functions import col, udf,desc
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql import functions
from pyspark.sql.functions import month, count

import sys
reload(sys)
sys.setdefaultencoding('utf8')

spark = SparkSession.builder.getOrCreate()

PATH ="/user/s2575760/project/data/enwiki-202012010-pages-meta-history/*"
df = spark.read.csv(PATH,header="true")

df0 = df.select("page_id", "page_title", "page_ns", "rev_id", "parent_revid", "model", "format", "text_size", "sha1", "contributor_ip", "contributor_user_id", "contributor_username", to_timestamp(df.timestamp, 'yyyy-MM-dd').alias('date'))

df1 = df0.filter((df0.date >= '2019-01-01') & (df0.date <= '2019-12-31'))
df2 = df1.select("page_id", "page_title", "rev_id", "date", "text_size").groupBy("page_id", "page_title", month("date").alias("month")).agg(count("rev_id").alias("count_revisions")).sort(desc('count_revisions'))

#Result Example
# +-------+--------------------+-----+---------------+
# |page_id|          page_title|month|count_revisions|
# +-------+--------------------+-----+---------------+
# | 100151|Index of music ar...|    8|           1514|
# |  57594|Talk:George Washi...|    2|           1179|
# |  40297|Wikipedia:Referen...|    4|           1080|
# |  40297|Wikipedia:Referen...|    7|           1055|
# |  40297|Wikipedia:Referen...|    9|           1031|
# |  40297|Wikipedia:Referen...|    6|            998|
# |  62233| Notre-Dame de Paris|    4|            951|
# |  51396|                2020|    1|            911|
# |  40297|Wikipedia:Referen...|   10|            898|
# |  40297|Wikipedia:Referen...|    3|            890|
# |  40297|Wikipedia:Referen...|    8|            851|
# |  58158|           Talk:Kyiv|    9|            821|
# |  40297|Wikipedia:Referen...|    2|            805|
# | 102446|           Huey Long|    7|            795|
# |  57594|Talk:George Washi...|    1|            793|
# |  63931|         Talk:Taiwan|    5|            757|
# |  40297|Wikipedia:Referen...|    5|            752|
# |  40297|Wikipedia:Referen...|   11|            733|
# |  65348|Talk:Michael Jackson|    3|            729|
# |  34059|         War of 1812|    7|            720|
# +-------+--------------------+-----+---------------+
