import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,when,lit
from pyspark.sql.types import *
import pyspark.sql.functions as fn


#------------------------load data--------------------#
sc = SparkSession.builder.appName("feature_data").getOrCreate()

train = sc.read.option("header",True).csv("Spark/in/train_format1.csv")
test = sc.read.option("header",True).csv("Spark/in/test_format1.csv")

info = sc.read.option("header",True).csv("Spark/in/user_info_format1.csv")
log = sc.read.option("header",True).csv("Spark/in/user_log_format1.csv")

#-------------------------null/0/2-----------------------#
# gender null->2
# age null->0

# info.na.fill(subset="gender", value=2)
# info.na.fill(subset="age_range", value=0)

info = info.withColumn("age_range", when(info.age_range.isNull(),lit("0")).otherwise(info.age_range))
info = info.withColumn("gender", when(info.gender.isNull(),lit("2")).otherwise(info.gender))

#-------------------------basic feature-----------------------#

# age, gender
train = train.join(info, on="user_id", how="left")
test = test.join(info, on="user_id", how="left")



#-------------------------select feature-----------------------#

#-------------------------user_seller feature-----------------------#

# total_logs: user in 'the' seller
total_logs= log.groupBy("user_id", "seller_id").count()
total_logs = total_logs.withColumnRenamed("count", "total_logs")
total_logs = total_logs.withColumnRenamed("seller_id", "merchant_id")
#pd_total_logs_temp = total_logs_temp.toPandas()

train = train.join(total_logs, on=["user_id", "merchant_id"], how="left")
train = train.withColumn("total_logs", when(train.total_logs.isNull(),lit("0")).otherwise(train.total_logs))

test = test.join(total_logs, on=["user_id", "merchant_id"], how="left")
test = test.withColumn("total_logs", when(test.total_logs.isNull(),lit("0")).otherwise(test.total_logs))


# clicks
clicks = log.filter("action_type==0")
clicks = clicks.groupBy("user_id","seller_id").count()
clicks = clicks.withColumnRenamed("count","clicks")
clicks = clicks.withColumnRenamed("seller_id","merchant_id")

train = train.join(clicks, on=["user_id","merchant_id"], how="left")
train = train.withColumn("clicks", when(train.clicks.isNull(),lit("0")).otherwise(train.clicks))

test = test.join(clicks, on=["user_id","merchant_id"], how="left")
test = test.withColumn("clicks", when(test.clicks.isNull(),lit("0")).otherwise(test.clicks))


# shopping_carts
shopping_carts = log.filter("action_type==1")
shopping_carts = shopping_carts.groupBy("user_id","seller_id").count()
shopping_carts = shopping_carts.withColumnRenamed("count","shopping_carts")
shopping_carts = shopping_carts.withColumnRenamed("seller_id","merchant_id")

train = train.join(shopping_carts, on = ["user_id","merchant_id"],how="left")
train = train.withColumn("shopping_carts", when(train.shopping_carts.isNull(),lit("0")).otherwise(train.shopping_carts))

test = test.join(shopping_carts, on = ["user_id","merchant_id"],how="left")
test = test.withColumn("shopping_carts", when(test.shopping_carts.isNull(),lit("0")).otherwise(test.shopping_carts))


# purchase_times
purchase_times = log.filter("action_type==2")
purchase_times = purchase_times.groupBy("user_id","seller_id").count()
purchase_times = purchase_times.withColumnRenamed("count","purchase_times")
purchase_times = purchase_times.withColumnRenamed("seller_id","merchant_id")

train = train.join(purchase_times, on = ["user_id","merchant_id"],how="left")
train = train.withColumn("purchase_times", when(train.purchase_times.isNull(),lit("0")).otherwise(train.purchase_times))

test = test.join(purchase_times, on = ["user_id","merchant_id"],how="left")
test = test.withColumn("purchase_times", when(test.purchase_times.isNull(),lit("0")).otherwise(test.purchase_times))


# favourite_times
favourite_times = log.filter("action_type==3")
favourite_times = favourite_times.groupBy("user_id","seller_id").count()
favourite_times = favourite_times.withColumnRenamed("count","favourite_times")
favourite_times = favourite_times.withColumnRenamed("seller_id","merchant_id")

train = train.join(favourite_times, on = ["user_id","merchant_id"],how="left")
train = train.withColumn("favourite_times", when(train.favourite_times.isNull(),lit("0")).otherwise(train.favourite_times))

test = test.join(favourite_times, on = ["user_id","merchant_id"],how="left")
test = test.withColumn("favourite_times", when(test.favourite_times.isNull(),lit("0")).otherwise(test.favourite_times))



# unique_item_ids:
unique_item_ids= log.groupBy("user_id", "seller_id", "item_id").count()
unique_item_ids = unique_item_ids.groupBy("user_id", "seller_id").count()
unique_item_ids = unique_item_ids.withColumnRenamed("seller_id", "merchant_id")
unique_item_ids = unique_item_ids.withColumnRenamed("count", "unique_item_ids")

train = train.join(unique_item_ids, on=["user_id", "merchant_id"], how="left")
train = train.withColumn("unique_item_ids", when(train.unique_item_ids.isNull(),lit("0")).otherwise(train.unique_item_ids))

test = test.join(unique_item_ids, on=["user_id", "merchant_id"], how="left")
test = test.withColumn("unique_item_ids", when(test.unique_item_ids.isNull(),lit("0")).otherwise(test.unique_item_ids))


# categories:
categories = log.groupBy("user_id", "seller_id", "cat_id").count()
categories = categories.groupBy("user_id", "seller_id").count()
categories = categories.withColumnRenamed("seller_id", "merchant_id")
categories = categories.withColumnRenamed("count", "categories")

train = train.join(categories, on=["user_id", "merchant_id"], how="left")
train = train.withColumn("categories", when(train.categories.isNull(),lit("0")).otherwise(train.categories))

test = test.join(categories, on=["user_id", "merchant_id"], how="left")
test = test.withColumn("categories", when(test.categories.isNull(),lit("0")).otherwise(test.categories))



# browse_days:
browse_days = log.groupBy("user_id", "seller_id", "time_stamp").count()
browse_days = browse_days.groupBy("user_id", "seller_id").count()
browse_days = browse_days.withColumnRenamed("seller_id", "merchant_id")
browse_days = browse_days.withColumnRenamed("count", "browse_days")

train = train.join(browse_days, on=["user_id", "merchant_id"], how="left")
train = train.withColumn("browse_days", when(train.browse_days.isNull(),lit("0")).otherwise(train.browse_days))

test = test.join(browse_days, on=["user_id", "merchant_id"], how="left")
test = test.withColumn("browse_days", when(test.browse_days.isNull(),lit("0")).otherwise(test.browse_days))


# browse_time_interval

time2 = log.filter("time_stamp>400" and "time_stamp<700")
browse_time2 = time2.groupBy("user_id","seller_id").count()
browse_time2 = browse_time2.withColumnRenamed("count","time2")
browse_time2 = browse_time2.withColumnRenamed("seller_id","merchant_id")
train = train.join(browse_time2, on=["user_id","merchant_id"], how="left")
train = train.withColumn("time2", when(train.time2.isNull(), lit("0")).otherwise(train.time2))
test = test.join(browse_time2, on=["user_id","merchant_id"], how="left")
test = test.withColumn("time2", when(test.time2.isNull(), lit("0")).otherwise(test.time2))

time3 = log.filter("time_stamp>700" and "time_stamp<1000")
browse_time3 = time3.groupBy("user_id","seller_id").count()
browse_time3 = browse_time3.withColumnRenamed("count","time3")
browse_time3 = browse_time3.withColumnRenamed("seller_id","merchant_id")
train = train.join(browse_time3, on=["user_id","merchant_id"], how="left")
train = train.withColumn("time3", when(train.time3.isNull(), lit("0")).otherwise(train.time3))
test = test.join(browse_time3, on=["user_id","merchant_id"], how="left")
test = test.withColumn("time3", when(test.time3.isNull(), lit("0")).otherwise(test.time3))


time4 = log.filter("time_stamp>1000")
browse_time4 = time4.groupBy("user_id","seller_id").count()
browse_time4 = browse_time4.withColumnRenamed("count","time4")
browse_time4 = browse_time4.withColumnRenamed("seller_id","merchant_id")
train = train.join(browse_time4, on=["user_id","merchant_id"], how="left")
train = train.withColumn("time4", when(train.time4.isNull(), lit("0")).otherwise(train.time4))
test = test.join(browse_time4, on=["user_id","merchant_id"], how="left")
test = test.withColumn("time4", when(test.time4.isNull(), lit("0")).otherwise(test.time4))



#----------------------------now describe user habit-----------------#
# purchase_divide_log:
# focus on users: describe one's purchasing habit
# a user: total purchase/total logs
user_total_log = log.groupBy("user_id").count()  # total logs for a user
user_total_log = user_total_log.withColumnRenamed('count', 'user_total_log')

user_purchase_log = log.filter("action_type==2")
user_purchase_log = user_purchase_log.groupBy("user_id").count()
user_purchase_log = user_purchase_log.withColumnRenamed("count","user_purchase_log")

#user_habit_log = user_total_log.join(user_purchase_log, on="user_id", how="left")

user_purchase_log = user_purchase_log.join(user_total_log, on="user_id",how="left")
user_habit = user_purchase_log.select("user_id", fn.col("user_total_log")/fn.col("user_purchase_log"))

train = train.join(user_habit, on="user_id", how="left")
test = test.join(user_habit, on="user_id", how="left")


# dedicated on brand
# how many brands a user has purchased
p_log = log.filter("action_type==2")
dedicated_on_brand = p_log.groupBy("user_id","brand_id").count()
dedicated_on_brand = dedicated_on_brand.groupBy("user_id").sum()
dedicated_on_brand = dedicated_on_brand.withColumnRenamed("sum(count)", "dedicated_on_brand")

train = train.join(dedicated_on_brand, on="user_id", how="left")
test = test.join(dedicated_on_brand, on="user_id", how="left")

# dedicated on seller
# how many sellers a user has purchased in
dedicated_on_seller = p_log.groupBy("user_id","seller_id").count()
dedicated_on_seller = dedicated_on_seller.groupBy("user_id").sum()
dedicated_on_seller = dedicated_on_seller.withColumnRenamed("sum(count)", "dedicated_on_seller")

train = train.join(dedicated_on_seller, on="user_id", how="left")
test = test.join(dedicated_on_seller, on="user_id", how="left")



# dedicated on cat
# how many times a user has purchased the same category
dedicated_on_cat = p_log.groupBy("user_id","cat_id").count()
dedicated_on_cat = dedicated_on_cat.groupBy("user_id").sum()
dedicated_on_cat = dedicated_on_cat.withColumnRenamed("sum(count)", "dedicated_on_cat")

train = train.join(dedicated_on_cat, on="user_id", how="left")
test = test.join(dedicated_on_cat, on="user_id", how="left")



#--------------------------now describe seller-------------------#

reg_train = train.filter("label==1")

# seller_size: total sales
seller_size = p_log.groupBy("seller_id").count()
seller_size = seller_size.withColumnRenamed("count","seller_size")
seller_size = seller_size.withColumnRenamed("seller_id","merchant_id")

train = train.join(seller_size,on="merchant_id", how="left")
train = train.withColumn("seller_size", when(train.seller_size.isNull(),lit("0")).otherwise(train.seller_size))
test = test.join(seller_size,on="merchant_id", how="left")
test = test.withColumn("seller_size", when(test.seller_size.isNull(),lit("0")).otherwise(test.seller_size))

# old_customer
old_customer = reg_train.groupBy("merchant_id").count()
old_customer = old_customer.withColumnRenamed("count","old_customer")
old_customer = old_customer.withColumnRenamed("seller_id","merchant_id")

train = train.join(old_customer, on="merchant_id", how="left")
train = train.withColumn("old_customer", when(train.old_customer.isNull(),lit("0")).otherwise(train.old_customer))
test = test.join(old_customer, on="merchant_id", how="left")
test = test.withColumn("old_customer", when(test.old_customer.isNull(),lit("0")).otherwise(test.old_customer))

# sell_item
sell_item = p_log.groupBy("seller_id","item_id").count()
sell_item = sell_item.groupBy("seller_id").count()
sell_item = sell_item.withColumnRenamed("count","sell_item")
sell_item = sell_item.withColumnRenamed("seller_id","merchant_id")

train = train.join(sell_item, on="merchant_id", how="left")
test = test.join(sell_item, on="merchant_id", how="left")
train = train.withColumn("sell_item", when(train.sell_item.isNull(),lit("0")).otherwise(train.sell_item))
test = test.withColumn("sell_item", when(test.sell_item.isNull(),lit("0")).otherwise(test.sell_item))


# sell_cate
sell_cate = p_log.groupBy("seller_id","cat_id").count()
sell_cate = sell_cate.groupBy("seller_id").count()
sell_cate = sell_cate.withColumnRenamed("count","sell_cate")
sell_cate = sell_cate.withColumnRenamed("seller_id","merchant_id")

train = train.join(sell_cate, on="merchant_id", how="left")
test = test.join(sell_cate, on="merchant_id", how="left")
train = train.withColumn("sell_cate", when(train.sell_cate.isNull(),lit("0")).otherwise(train.sell_cate))
test = test.withColumn("sell_cate", when(test.sell_cate.isNull(),lit("0")).otherwise(test.sell_cate))



# sell_brand
sell_brand = p_log.groupBy("seller_id","brand_id").count()
sell_brand = sell_brand.groupBy("seller_id").count()
sell_brand = sell_brand.withColumnRenamed("count","sell_brand")
sell_brand = sell_brand.withColumnRenamed("seller_id","merchant_id")

train = train.join(sell_brand, on="merchant_id", how="left")
test = test.join(sell_brand, on="merchant_id", how="left")
train = train.withColumn("sell_brand", when(train.sell_cate.isNull(),lit("0")).otherwise(train.sell_brand))
test = test.withColumn("sell_brand", when(test.sell_brand.isNull(),lit("0")).otherwise(test.sell_brand))








