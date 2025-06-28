from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql import Window

# 初始化SparkSession# 在 Spark 3.0 及以上版本中，LEGACY 模式是宽松解析，兼容旧格式（如单数字日期（如 9-Dec-13 而非 09-Dec-13））#
spark = SparkSession.builder.master("local[*]")\
                            .appName("ReadCSV") \
                            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                            .getOrCreate()

# 读取CSV文件，设置选项，例如header为True表示第一行是列名，inferSchema为True自动推断数据类型
stocks = spark.read.csv("file:///Users/dai/PycharmProjects/VaR/factors/stocks/", header="true", inferSchema="true")

# 新增列 文件名
stocks = stocks.withColumn("Symbol", fun.input_file_name()) \
               .withColumn("Symbol", fun.element_at(fun.split("Symbol", "/"), -1)) \
               .withColumn("Symbol", fun.element_at(fun.split("Symbol", "\."), 1))

# 新增列 count
stocks = stocks.withColumn('count', fun.count('Symbol').over(Window.partitionBy('Symbol'))) \
               .filter(fun.col('count') > 260*5 + 10)

stocks = stocks.withColumn('Date', fun.to_date(fun.to_timestamp(fun.col('Date'), 'dd-MMM-yy')))

# stocks.printSchema()

stocks2 = stocks.filter(fun.col('Date').between('2009-10-23', '2014-10-23'))
stocks_pd_df = stocks2.toPandas()       #将PySpark DataFrame 转换为 pandas DataFrame，在内存中执行操作来轻松地继续使用它
print(stocks_pd_df.head(5))
# 显示数据
stocks2.show(9)