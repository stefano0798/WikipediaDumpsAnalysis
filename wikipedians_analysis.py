from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import logging

from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import ceil
from pyspark.sql.functions import floor
from pyspark.sql.functions import sum
from pyspark.sql.functions import col
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import expr


# from pyspark.sql.functions import to_timestamp

logger = logging.getLogger('py4j')

source_data = '/user/s2575760/project/data/enwiki-202012010-pages-meta-history/enwiki-20201201-pages-meta-history*.csv'
source_data_df = spark.read.csv(source_data, header='true', inferSchema=True)
source_data_df = source_data_df.withColumn("timestamp_typecast",
                                           source_data_df.timestamp.cast('timestamp'))\
    .drop("timestamp").withColumnRenamed("timestamp_typecast", "timestamp")

# list of bots to filter
botlist_data = '/user/s2575760/project/data/botlist.csv'
botlist = spark.read.csv(botlist_data, header='true')

# filter registered users
# consider only users which have registered on wikipedia
registered_users_data = source_data_df.filter(source_data_df.contributor_username != "na")\
    .select("page_id", "page_title", "page_ns", "rev_id", "timestamp", "text_size", "contributor_user_id", "contributor_username")\
    .join(broadcast(botlist), source_data_df.contributor_username == botlist.bot_name, how='left')
# and remove users which are in the bot list
registered_users_data = registered_users_data.filter(registered_users_data.bot_name.isNull())

# calculating total number of edits for each user
# classifying >250 edits as 'wikipedian'
users_num_edits = registered_users_data.groupBy("contributor_username").count().withColumnRenamed("count", "edit_count")
users_num_edits = users_num_edits.withColumn("wikipedian", users_num_edits.edit_count > 250).withColumnRenamed("contributor_username", "username")

users_num_edits_gt250 = users_num_edits.filter(users_num_edits.edit_count > 250) .coalesce(1)
users_num_edits_lt250 = users_num_edits.filter(users_num_edits.edit_count < 250) .coalesce(1)
users_num_edits_gt100lt250 = users_num_edits.filter(users_num_edits.edit_count < 250).\
     filter(users_num_edits.edit_count > 100).coalesce(1)
users_num_edits_gt10lt100 = users_num_edits.filter(users_num_edits.edit_count < 100).\
     filter(users_num_edits.edit_count > 10).coalesce(1)
users_num_edits_lt10 = users_num_edits.filter(users_num_edits.edit_count < 10).coalesce(1)

users_num_edits_gt250.write.format('csv').mode("overwrite").option("header", "true").save("/user/s2575760/project/analysis/wikipedians/users_num_edits_all_gt250_rmbot.csv")
users_num_edits_gt100lt250.write.format('csv').mode("overwrite").option("header", "true").save("/user/s2575760/project/analysis/wikipedians/users_num_edits_gt100lt250_rmbot.csv")
users_num_edits_gt10lt100.write.format('csv').mode("overwrite").option("header", "true").save("/user/s2575760/project/analysis/wikipedians/users_num_edits_gt10lt100_rmbot.csv")
users_num_edits_lt10.write.format('csv').mode("overwrite").option("header", "true").save("/user/s2575760/project/analysis/wikipedians/users_num_edits_lt10_rmbot.csv")

# getting earliest edit of a user - this marks the beginning of the life of the user
# this is considered as Day 1 for the user
users_first_edit = registered_users_data.withColumn("rev_timestamp_unix", unix_timestamp("timestamp"))\
    .groupBy("contributor_username").min("rev_timestamp_unix").withColumnRenamed("min(rev_timestamp_unix)", "first_edit_unix")\
    .withColumnRenamed("contributor_username", "username")#\

# attribute is added to each edit to mark which day of the editor's life the edit was made
edits_days_since_first = registered_users_data.join(users_first_edit, registered_users_data.contributor_username == users_first_edit.username)\
    .withColumn("rev_timestamp_unix", unix_timestamp("timestamp")).drop(col("username"))
edits_days_since_first = edits_days_since_first.withColumn("day_n", floor((edits_days_since_first.rev_timestamp_unix -
                                                           edits_days_since_first.first_edit_unix)/86400) + 1 )\
    .drop("timestamp", "first_edit_unix", "rev_timestamp_unix")
edits_days_since_first = edits_days_since_first.join(users_num_edits,
                                                     edits_days_since_first.contributor_username == users_num_edits.username)\
    .drop("username", "edit_count")


"""
Plot 1: Active editors on day n
"""

# Grouping wikipedians and non-wikipedians and calculating percentage of editors which made edits on a given day
active_editors_on_day_n = edits_days_since_first.select("contributor_username", "day_n", "wikipedian").distinct()\
     .groupBy("day_n", "wikipedian").count().withColumnRenamed("count", "editor_count")

num_total_editors = users_num_edits.select("username", "wikipedian").groupBy("wikipedian").count()\
     .withColumnRenamed("count", "total_editor_count").withColumnRenamed("wikipedian", "wiki")

active_editors_percent_on_day_n = active_editors_on_day_n.join(num_total_editors, num_total_editors.wiki == active_editors_on_day_n.wikipedian)

active_editors_percent_on_day_n = active_editors_percent_on_day_n.withColumn("percent_editor_count",
                                                                              active_editors_percent_on_day_n.editor_count /
                                                                              active_editors_percent_on_day_n.total_editor_count)

active_editors_percent_on_day_n.write.format('csv').mode("overwrite").option("header", "true").save("/user/s2575760/project/analysis/wikipedians/active_editors_on_day_n_att5_rmbot.csv")


"""
Plot 2: Mean number of edits on day n
"""

# Grouping wikipedians and non-wikipedians
edits_per_user_day_n = edits_days_since_first.groupBy("day_n", "contributor_username", "wikipedian").count()\
    .withColumnRenamed("count", "edit_count")
mean_edits_day_n = edits_per_user_day_n.groupBy("day_n", "wikipedian").mean("edit_count").coalesce(1)
mean_edits_day_n.write.format('csv').mode('overwrite').option("header", "true").save("/user/s2575760/project/analysis/wikipedians/mean_edits_on_day_n_all_rmbot.csv")


"""
Plot 3: Mean edits on day n - editor categories
"""

users_edit_levels = users_num_edits.withColumn("editor_level", expr("CASE WHEN edit_count < 100 THEN 1 " +
                                                                    "WHEN edit_count < 1000 THEN 2 " +
                                                                    "WHEN edit_count < 5000 THEN 3 " +
                                                                    "WHEN edit_count < 10000 THEN 4 " +
                                                                    "WHEN edit_count > 10000 THEN 5 " +
                                                                    "ELSE 0 END")   ).drop("edit_count")

edits_per_user_by_level_day_n = edits_per_user_day_n.join(users_edit_levels, edits_per_user_day_n.contributor_username == users_edit_levels.username)
mean_edits_by_level_day_n = edits_per_user_by_level_day_n.groupBy("day_n", "editor_level").mean("edit_count").coalesce(1)
mean_edits_by_level_day_n.write.format('csv').mode('overwrite').option("header", "true").save("/user/s2575760/project/analysis/wikipedians/mean_edits_on_day_n_editor_level_all_rmbot.csv")