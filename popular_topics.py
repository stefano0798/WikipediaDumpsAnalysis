# example of question3 highlight various topics that gained popularity over the years
from pyspark import SparkContext
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from datetime import timedelta
from pyspark.sql.functions import col, udf,desc
from pyspark.sql.functions import to_timestamp

spark = SparkSession.builder.getOrCreate()

PATH ="/user/s2575760/project/data/sample/preprocesed/wiki_fields_enwiki-20201201-pages-meta-history2.xml-p151386p151573.csv"
df = spark.read.csv(PATH,header="true")

df1 = df.select('page_id','page_title',to_timestamp(df.timestamp, 'yyyy-MM-dd').alias('date'))

#set start_time
time_2002 = datetime(2002, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')


time_2014 = datetime(2014, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2015 = datetime(2015, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2016 = datetime(2016, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2017 = datetime(2017, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2018 = datetime(2018, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2019 = datetime(2019, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2020 = datetime(2020, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

#filter by time
df_2002 = df1.where(df1.date <= time_2002)

data_title_count_2002 = df_2002.groupBy(['page_id', df.page_title]).count().withColumnRenamed("count", "number")

#sort by current number
data_title_count_2002 = data_title_count_2002.sort(desc("number"))

#show the most popular title in 2002
print(data_title_count_2002.show())


# future work : code other part and write as a function


df_2015 = df1.where(df1.date > time_2014).where(df1.date <= time_2015)
data_title_count_2015 = df_2015.groupBy(['page_id', df.page_title]).count().withColumnRenamed("count", "number")
data_title_count_2015 = data_title_count_2015.sort(desc("number"))
ans2015 = data_title_count_2015.head(10)
print(ans2015)

#user

df2 = df.select('page_id','page_title','contributor_ip','contributor_user_id','contributor_username',to_timestamp(df.timestamp, 'yyyy-MM-dd').alias('date'))
df2_2020 = df2.where(df2.date > time_2019).where(df2.date <= time_2020)
data_user_2020 = df2_2020.groupBy(['contributor_user_id','contributor_username','page_title']).count().withColumnRenamed("count", "number")
data_user_2020 = data_user_2020.sort(desc("number"))
print(data_user_2020.show())
