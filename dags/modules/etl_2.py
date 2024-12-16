import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sqlalchemy import create_engine
import psycopg2
import os
import pandas as pd

spark = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
        .master("local") \
        .appName("PySpark_Postgresql").getOrCreate()

def extract_transform_total_film():
    df_film = spark.read.format("jdbc") \
          .option("url", "jdbc:postgresql://34.56.65.122:5231/project3") \
          .option("driver", "org.postgresql.Driver") \
          .option("dbtable", "film") \
          .option("user", "postgres") \
          .option("password", "ftdebatch3").load()
    df_film.createOrReplaceTempView("film")

    df_film_category = spark.read.format("jdbc") \
          .option("url", "jdbc:postgresql://34.56.65.122:5231/project3") \
          .option("driver", "org.postgresql.Driver") \
          .option("dbtable", "film_category") \
          .option("user", "postgres") \
          .option("password", "ftdebatch3").load()
    df_film_category.createOrReplaceTempView("film_category")

    df_category = spark.read.format("jdbc") \
          .option("url", "jdbc:postgresql://34.56.65.122:5231/project3") \
          .option("driver", "org.postgresql.Driver") \
          .option("dbtable", "category") \
          .option("user", "postgres") \
          .option("password", "ftdebatch3").load()
    df_category.createOrReplaceTempView("category")

    df_result = spark.sql('''
        SELECT 
        c.name AS category_name,
        COUNT(f.film_id) AS total_film,
        current_date() AS date,
        'davin' AS data_owner
        FROM category c
        JOIN film_category fc ON c.category_id = fc.category_id
        JOIN film f ON fc.film_id = f.film_id
        GROUP BY c.name
        ORDER BY total_film DESC
    ''')
    
    df_result.write.mode("overwrite") \
      .partitionBy("date") \
      .option('compression', 'snappy') \
      .option('partitionOverwriteMode', 'dynamic') \
      .save('data_result_2')


def load_total_film():
    df = pd.read_parquet('data_result_2')
    engine = create_engine(
        'mysql+mysqlconnector://4FFFhK9fXu6JayE.root:9v07S0pKe4ZYCkjE@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/project3',
        echo=False)
    df.to_sql(name='total_film_by_category', con=engine, if_exists='append')