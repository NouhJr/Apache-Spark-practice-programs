# Apache Spark (pyspark package) practice programs
  # 1- Word Count program:
  Here are the brief steps for writing the word counting program:
  Create a base RDD from Complete_Shakespeare.txt file.
  Use RDD transformation to create a long list of words from each element of the base RDD.
  Remove stop words from your data.
  Create pair RDD where each element is a pair tuple of ('w', 1)
  Group the elements of the pair RDD by key (word) and add up their values.
  Swap the keys (word) and values (counts) so that keys is count and value is the word.
  Finally, sort the RDD by descending order and print the 10 most frequent words and their frequencies.
  (*) commit url: https://github.com/NouhJr/Apache-Spark-practice-programs/commit/a99a4a5efa36bd7830f5eea820ce0e326ff25588
  https://github.com/NouhJr/Apache-Spark-practice-programs/commit/26579f2b12745a8f51cf54c6320bb09e9f1a2dbc
  
  # 2- Spam Filter program:
  Here are the brief steps for creating a spam classifier:
  Create an RDD of strings representing email.
  Run MLlib’s feature extraction algorithms to convert text into an RDD of vectors.
  Call a classification algorithm on the RDD of vectors to return a model object to classify new points.
  Evaluate the model on a test dataset using one of MLlib’s evaluation functions.
  (*) commit url: https://github.com/NouhJr/Apache-Spark-practice-programs/commit/c5d521a7e7e33bd8436efcd63f4ea1b0d15f93fb
