from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

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

    spark = SparkSession.builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Leitura dos dados....")
    df01 = (        
        spark
        .read
        .format("csv")
        .options(header=True, inferSchema=True, delimiter=";", encoding="UTF-8")
        .load("s3a://dl-landing-zone-539445819059/enade2017/microdados2017_arq1.txt")
    )

    df03 = (
        spark
        .read
        .format("csv")
        .options(header=True, inferSchema=True, delimiter=";", encoding="UTF-8")
        .load("s3a://dl-landing-zone-539445819059/enade2017/microdados2017_arq3.txt")
    )

    df05 = (
        spark
        .read
        .format("csv")
        .options(header=True, inferSchema=True, delimiter=";", encoding="UTF-8")
        .load("s3a://dl-landing-zone-539445819059/enade2017/microdados2017_arq5.txt")
    )

    df06 = (
        spark
        .read
        .format("csv")
        .options(header=True, inferSchema=True, delimiter=";", encoding="UTF-8")
        .load("s3a://dl-landing-zone-539445819059/enade2017/microdados2017_arq6.txt")
    )

    print("Tratamento dos dados...")

    df01 = df01.orderBy(f.col('CO_CURSO').asc()).withColumn("index", row_number().over(Window.partitionBy().orderBy(df01['CO_CURSO'])))
    df03 = df03.orderBy(f.col('CO_CURSO').asc()).withColumn("index", row_number().over(Window.partitionBy().orderBy(df03['CO_CURSO'])))
    df05 = df05.orderBy(f.col('CO_CURSO').asc()).withColumn("index", row_number().over(Window.partitionBy().orderBy(df05['CO_CURSO'])))
    df06 = df06.orderBy(f.col('CO_CURSO').asc()).withColumn("index", row_number().over(Window.partitionBy().orderBy(df06['CO_CURSO'])))

    df0103 = df01.join(df03, [df01.index == df03.index]).drop(df03.index).drop(df03.NU_ANO).drop(df03.CO_CURSO)
    df010305 = df0103.join(df05, [df0103.index == df05.index]).drop(df05.index).drop(df05.NU_ANO).drop(df05.CO_CURSO)
    df01030506 = df010305.join(df06, [df010305.index == df06.index]).drop(df06.index).drop(df06.NU_ANO).drop(df06.CO_CURSO)
   
    print("Escrita dos dados...")
    (
        df01030506
        .write
        .parquet("s3a://dl-processing-zone-539445819059/enade2017/", mode="overwrite")
    )


    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()