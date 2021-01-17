# Importing spark context from pyspark package
from pyspark import SparkContext
# Importing Spark Session
from pyspark.sql import SparkSession
# Importing classes from pyspark.mllib.recommendation submodule
from pyspark.mllib.clustering import *
# Importing 'sqrt' from 'math' package to calculate 'error()' function.
from math import sqrt
# Importing matplotlib.pyplot as plt
import matplotlib.pyplot as plt
# Importing pandas package
import pandas as pd

# Creating spark context as 'sc'
sc = SparkContext("local", "Spam Filter")

spark = SparkSession.builder.getOrCreate()

# Load the dataset into a RDD
clusterRDD = sc.textFile("5000_points.txt")

# Split the RDD based on tab
rdd_split = clusterRDD.map(lambda x: x.split("\t"))

# Transform the split RDD by creating a list of integers
rdd_split_int = rdd_split.map(lambda x: [int(x[0]), int(x[1])])

# Count the number of rows in RDD
print("There are {} rows in the rdd_split_int dataset".format(rdd_split_int.count()))

# Implementing function 'error()' to evaluate the k-means model
def error (point):
    center = model.centers[model.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))


# Train the model with clusters from 13 to 16 and compute WSSSE
for clst in range(13, 17):
    model = KMeans.train(rdd_split_int, clst, seed=1)
    WSSSE = rdd_split_int.map(lambda point: error(point)).reduce(lambda x, y: x + y)
    print("The cluster {} has Within Set Sum of Squared Error {}".format(clst, WSSSE))

# Train the model again with the best k
model = KMeans.train(rdd_split_int, k=15, seed=1)

# Get cluster centers
cluster_centers = model.clusterCenters

# Convert rdd_split_int RDD into Spark DataFrame
rdd_split_int_df = spark.createDataFrame(rdd_split_int, schema=["col1", "col2"])

# Convert Spark DataFrame into Pandas DataFrame
rdd_split_int_df_pandas = rdd_split_int_df.toPandas()

# Convert "cluster_centers" that you generated earlier into Pandas DataFrame
cluster_centers_pandas = pd.DataFrame(cluster_centers, columns=["col1", "col2"])

# Create an overlaid scatter plot
plt.scatter(rdd_split_int_df_pandas["col1"], rdd_split_int_df_pandas["col2"])
plt.scatter(cluster_centers_pandas["col1"], cluster_centers_pandas["col2"], color="red", marker="x")
plt.show()