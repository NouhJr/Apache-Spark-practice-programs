# Word Count With Apache Spark (pyspark package)
Here are the brief steps for writing the word counting program:
Create a base RDD from Complete_Shakespeare.txt file.
Use RDD transformation to create a long list of words from each element of the base RDD.
Remove stop words from your data.
Create pair RDD where each element is a pair tuple of ('w', 1)
Group the elements of the pair RDD by key (word) and add up their values.
Swap the keys (word) and values (counts) so that keys is count and value is the word.
Finally, sort the RDD by descending order and print the 10 most frequent words and their frequencies.
