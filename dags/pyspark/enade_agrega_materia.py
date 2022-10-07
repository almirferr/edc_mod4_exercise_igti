from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

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

    df = (
        spark
        .read
        .format("parquet")
        .load("s3a://dl-processing-zone-539445819059/enade2017/")
    )
    
    print("****************")
    print("* AGREGA SEXO  *")
    print("****************")

    uf_idade = (
        df
        .groupby("TP_SEXO")
        .agg(f.mean('NU_IDADE').alias('media'))  
    )

    (
        uf_idade
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://dl-processing-zone-539445819059/intermediarias/tp_sexo")
    )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()
    