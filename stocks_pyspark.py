from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql import Window
from datetime import datetime
import pandas

# 初始化SparkSession
spark = SparkSession.builder.master("local[*]").appName("ReadCSV").getOrCreate()

# 读取CSV文件，设置选项，例如header为True表示第一行是列名，inferSchema为True自动推断数据类型
# stocks = spark.read.option("header", "true") \
#              .option("inferSchema", "true") \
             # .csv("file:///Users/dai/PycharmProjects/VaR/factors/stocks/GOOGL.csv")
stocks = spark.read.csv("file:///Users/dai/PycharmProjects/VaR/factors/stocks/", header="true", inferSchema="true")

# 新增列 文件名
stocks = stocks.withColumn("Symbol", fun.input_file_name()) \
               .withColumn("Symbol", fun.element_at(fun.split("Symbol", "/"), -1)) \
               .withColumn("Symbol", fun.element_at(fun.split("Symbol", "\."), 1))

# 新增列 count
stocks = stocks.withColumn('count', fun.count('Symbol').over(Window.partitionBy('Symbol'))) \
               .filter(fun.col('count') > 260*5 + 10)

# stocks = stocks.withColumn('Date', fun.to_date(fun.to_timestamp(fun.col('Date'), 'dd-MM-yy')))
stocks = stocks.withColumn('Date', fun.to_date(fun.to_timestamp(fun.col('Date'), 'dd-MMM-yy')))
# stocks.printSchema()

# stocks2 = stocks.filter(fun.col('Date') >= datetime(2009,10,23)) \
#                .filter(fun.col('Date') <= datetime(2014,10,23))
stocks2 = stocks.filter(fun.col('Date').between('2009-10-23', '2014-10-23'))

# 显示数据
stocks2.show(7)