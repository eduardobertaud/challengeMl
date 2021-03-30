"""
Loading important package of spark
"""
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.functions import rank, col
#from py4j.protocol import Py4JJavaError
import os

"""
Spark session creater
"""
st = SparkSession \
        .builder \
        .appName('ChallengeML') \
        .getOrCreate()

"""
Load data function for loading data..
@param -
        path - path of file
        header_value - header value, incase true first row will be header

@return - dataframe of loaded intended data.
"""

def load_data(path,header_value):
  df = st.read.csv(path,inferSchema=True,header=header_value)
  return df

"""
Functions to get nb_previous_ratings and avg_ratings_previous..
@param -
        dataFrame - Data frame with rates

@return - dataframe of loaded intended data.
"""
def nb_previous_ratings(df):
    df=df.withColumn("nb_previous_ratings",rank().over(Window.partitionBy("userId").orderBy("timestamp")) -1 )
    return df

def avg_ratings_previous(df):
  WindowSpec = Window.partitionBy("userId").rowsBetween(Window.unboundedPreceding,-1)

  df = df.withColumn("avg_ratings_previous", F.avg(F.col("rating")).over(WindowSpec.orderBy("userId", "timestamp")))
  return df

"""
Load data function for validate if file already exist..
@param -
        path - path of file

@return - boolean.
"""
def path_exist(path):
    if os.path.isdir(path):
        return True
    else:
        return False

def getUser(id):
    if path_exist("/Users/eduardobertaud/Desktop/challengeML/new_ratings.parquet"):
        parquetFile = st.read.parquet("new_ratings.parquet")
        parquetFile.createOrReplaceTempView("parquetFile")
        teenagers = st.sql("SELECT userId FROM parquetFile WHERE userId = 1")
        return teenagers.show(truncate=False)
    else:

        df = load_data('/Users/eduardobertaud/Desktop/challengeML/rating.csv',True)

        """
        calling Function nb_previous_rating and assign to dataFrame
        """
        df = nb_previous_ratings(df)

        """
        calling Function nb_previous_rating and assign to dataFrame
        """
        df = avg_ratings_previous(df)
        """
        Creating a new parquet file with the changes created
        """
        #df.write.parquet("new_ratings.parqet",mode='overwrite')
        df.write.mode('overwrite').parquet("/Users/eduardobertaud/Desktop/challengeML/new_ratings.parquet")
