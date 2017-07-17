from pyspark.sql import SparkSession
from pyspark.sql import Row

def loadMovieNames():
	movieNames = {}
	with open('../ml-100k/u.item') as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1]
	return movieNames

def Configuration():
	return SparkSession.builder.appName('popular-movie-dataframe').getOrCreate()

nameDic = loadMovieNames();
spark = Configuration();
lines = spark.sparkContext.textFile('../ml-100k/u.data')

movies = lines.map(lambda x: Row(movieID = int(x.split()[1])))

movieData = spark.createDataFrame(movies)

topMovie = movieData.groupBy('movieID').count().orderBy('count', ascending = False).cache()

topMovie.show()

movietop10 = topMovie.take(20)

print

for movie in movietop10:
	print "{0}:{1}".format(nameDic[movie[0]], movie[1])

spark.stop()