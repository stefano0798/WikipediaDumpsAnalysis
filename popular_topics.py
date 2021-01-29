# example of question3 highlight various topics that gained popularity over the years
from pyspark import SparkContext
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from datetime import timedelta
from pyspark.sql.functions import col, udf,desc
from pyspark.sql.functions import to_timestamp

spark = SparkSession.builder.getOrCreate()

PATH ="/user/s2575760/project/data/enwiki-202012010-pages-meta-history/*"
df = spark.read.csv(PATH,header="true")


df = df.filter(~df.page_title.like('%Wikipedia:%')).filter(~df.page_title.like('%List of:%')).filter(~df.page_title.like('% User:%')).filter(~df.page_title.like('% Template:%')).filter(~df.page_title.like('%Template talk:%')).filter(~df.page_title.like('%Wikipedia talk:%')).filter(~df.page_title.like('% Portal:%')).filter(~df.page_title.like('%User talk:%')).filter(~df.page_title.like('% Talk:%')).filter(~df.page_title.like('% Category:%'))

df1 = df.select('page_id','page_title',to_timestamp(df.timestamp, 'yyyy-MM-dd').alias('date'))


#set start_time
time_2002 = datetime(2002, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2003 = datetime(2003, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2004 = datetime(2004, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2005 = datetime(2005, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2006 = datetime(2006, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2007 = datetime(2007, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2008 = datetime(2008, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2009 = datetime(2009, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2010 = datetime(2010, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2011 = datetime(2011, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2012 = datetime(2012, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2013 = datetime(2013, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2014 = datetime(2014, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2015 = datetime(2015, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2016 = datetime(2016, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2017 = datetime(2017, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2018 = datetime(2018, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2019 = datetime(2019, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')

time_2020 = datetime(2020, 12, 31, 00, 00, 00).strftime(format='%Y-%m-%d')



#filter by time
df_2002 = df1.where(df1.date <= time_2002)
#df_2004 = df1.where(df1.date > time_2003).where(df1.date <= time_2004)

data_title_count_2002 = df_2002.groupBy(['page_id', df.page_title]).count().withColumnRenamed("count", "number")

#data_title_count_2004 = df_2004.groupBy(['page_id', df.page_title]).count().withColumnRenamed("count", "number")

#sort by current number
data_title_count_2002 = data_title_count_2002.sort(desc("number"))

#show the most popular title in 2002
print(data_title_count_2002.show())


# future work : code other part and write as a function




df_2010 = df1.where(df1.date > time_2009).where(df1.date <= time_2010)
data_title_count_2010 = df_2010.groupBy(['page_id', df.page_title]).count().withColumnRenamed("count", "number")
data_title_count_2010 = data_title_count_2010.sort(desc("number"))

ans2010 = data_title_count_2010.head(20)
print(ans2010)
print(data_title_count_2010.show())
data_title_count_2010.format('csv').mode("overwrite").option("header","true").save('/user/s2633434/data_title_count_2010.csv')


df_2020 = df1.where(df1.date > time_2019).where(df1.date <= time_2020)
data_title_count_2020 = df_2020.groupBy(['page_id', df.page_title]).count().withColumnRenamed("count", "number")
data_title_count_2020 = data_title_count_2020.sort(desc("number"))

ans2020 = data_title_count_2020.head(20)
print(ans2020)
print(data_title_count_2020.show())
data_title_count_2020.format('csv').mode("overwrite").option("header","true").save('/user/s2633434/data_title_count_2020.csv')

#user

df2 = df.select('page_id','page_title','contributor_ip','contributor_user_id','contributor_username',to_timestamp(df.timestamp, 'yyyy-MM-dd').alias('date'))
df2_2020 = df2.where(df2.date > time_2019).where(df2.date <= time_2020)
data_user_2020 = df2_2020.groupBy(['contributor_user_id','contributor_username','page_title']).count().withColumnRenamed("count", "number")
data_user_2020 = data_user_2020.sort(desc("number"))

data_user_2020.format('csv').mode("overwrite").option("header","true").save('/user/s2633434/d data_user_2020.csv')

print(data_user_2020.show())





