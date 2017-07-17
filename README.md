# MovieSimilarity
Figuring out movie Similarity using Movie Lens Dataset using Apache Spark

This small program was written to analyse the Movie Lens dataset and find out the similar movies provided the movie ID. I
have used ratings of 100,000 users form movies provided by u.ITEM in ml-100k directory. The results are written into movie-sim 
folder. In order to run the program:

Provide arguments in the following order: 
1. Movie Name 
2. Similarity Functions (optional)

Option for similarity functions are: a. CosineSimilarity b. Jaccard similarity c. Pearson Similairy

The resultset will contain the top 10 movies predicted using our similarity function with their score and strength.

Use the following syntax on the command line to run the program:

spark-submit MovieSimilarity.py 50 b

where, 50 corresponds to movie Star Wars and b corresponds to option Jaccard Similarity
