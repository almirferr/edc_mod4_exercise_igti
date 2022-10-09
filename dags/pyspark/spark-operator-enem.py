from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# set conf
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("Repartition Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("*****************")
    print("Iniciando!!!")
    print("*****************")

    url = 'https://raw.githubusercontent.com/almirferr/edc_mod4_exercise_igti/dev/titanic.csv'
    from pyspark import SparkFiles
    spark.sparkContext.addFile(url)
    df = spark.read.csv("file://"+SparkFiles.get("titanic.csv"), header=True, inferSchema=True)
#    df = (
#        spark
#        .read
#        .format("csv")
#        .options(header='true', inferSchema='true', delimiter=';')
#        #.load("s3a://dl-landing-zone-539445819060/enem/")
#        .load("https://raw.githubusercontent.com/almirferr/edc_mod4_exercise_igti/dev/titanic.csv")
#    )
    
    df.printSchema()
#
#    (df
#    .write
#    .mode("overwrite")
#    .format("parquet")
##    .save("s3a://dl-processing-zone-539445819060/enem/")
#    .save("s3://dl-processing-zone-539445819059/teste/")    
#    )

    print("*****************")
    print("Escrito com sucesso!")
    print("*****************")

    spark.stop()