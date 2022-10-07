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
            .appName("ENADE Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df_co_uf_curso = (
        spark
        .read
        .format("parquet")
        .load("s3a://dl-processing-zone-539445819059/intermediarias/co_uf_curso")
    )

    df_tp_sexo = (
        spark
        .read
        .format("parquet")
        .load("s3a://dl-processing-zone-539445819059/intermediarias/TP_sexo")
    )
   
    print("****************")
    print("* JOIN FINAL *")
    print("****************")

    df_final = (
        df_co_uf_curso
        .join(df_tp_sexo, on="index", how="inner")
    )

    (
        df_final
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-consumer-zone-539445819059/enade2017")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()
    