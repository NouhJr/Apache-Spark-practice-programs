# Import necessary submodules from PySpark package
from pyspark import SparkContext

# Creating spark context named 'sc'
sc = SparkContext("local", "Word Count")

# Storing file path in variable named 'file_path'
file_path = "D:\FCIH-Materials\Big Data\Courses\Spark\Projects\The Complete Works of William Shakespeare.txt"

# Creating the base RDD from our text file named 'baseRDD', using 'textFile()' function 
baseRDD = sc.textFile(file_path)

# Creating new RDD from 'baseRDD' named 'splitRDD' which contains splited lines into words using 'flatMap()' function
splitRDD = baseRDD.flatMap(lambda x: x.split(" "))

# Creating 'stop_words' list, contains all the stop words we want to remove
stop_words = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 'can', 'will', 'just', 'don', 'should', 'now']

# Removing stop words from 'splitRDD' using 'filter()' function
splitRDD_filtered = splitRDD.filter(lambda word: word.lower() not in stop_words)

# Creating pair RDD named 'splitRDD_With_No_StopWords' from 'splitRDD_filtered' using 'map()' function
splitRDD_With_No_StopWords = splitRDD_filtered.map(lambda w: (w, 1))

# Count of the number of occurences of each word in 'splitRDD_With_No_StopWords' using 'reduceByKey()' function and store it in new RDD named 'reducedRDD'
reducedRDD = splitRDD_With_No_StopWords.reduceByKey(lambda x,y: x + y)

# Swap the keys and values
swappedRDD = reducedRDD.map(lambda word: (word[1], word[0]))

# Sort the keys in descending order using 'sortByKey()' function
sortedRDD = swappedRDD.sortByKey(ascending=False)

# Show the top 10 most frequent words and their frequencies using 'take(N)' action
for word in sortedRDD.take(10):
    print("{} has {} counts". format(word[1], word[0]))
