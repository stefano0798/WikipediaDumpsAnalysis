from pyspark import SparkContext

sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

df = spark.read.csv("/user/s2575760/project/data/sample/preprocesed/wiki_fields_enwiki-20201201-pages-meta-history2.xml-p151386p151573.csv", header="true")

data = df.select(df.page_id, df.rev_id)

data = data.groupBy(data.page_id)
data = data.count()
data = data.orderBy("count", ascending=False) 

data = df.select(df.page_title, df.rev_id)
data = data.groupBy(data.page_title)
data = data.count()
data = data.orderBy("count", ascending=False)
data.show(5)