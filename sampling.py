from pyspark.sql.functions import col, size, split
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
hadoop = spark._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('/data/twitterNL/*/*')


for f in fs.get(conf).globStatus(path):
    filepath = str(f.getPath()).replace('hdfs://ctit048.ewi.utwente.nl', '') 
    sample = spark.read.json(filepath) \
        .select(col('text').alias('text')) \
        .filter(size(split(col('text'), ' ')) > 1) \
        .sample(0.025) \
        .write.option('compression', 'gzip').json('hdfs:///user/s2154854' + filepath)

    
