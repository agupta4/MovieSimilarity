import sys
from pyspark import SparkConf, SparkContext
from similarity import computeCosineSimilarity, perarsonSim, JaccardSim

class Processinfo(object):
    
    def __init__(self, name):
       self.connection = SparkConf().setMaster("local[*]").setAppName(name)
    
    def loadMovieNames(self):
        movieNames = {}
        with open("../ml-100k/u.ITEM") as f:
            for line in f:
                fields = line.split('|')
                movieNames[int(fields[0])] = fields[1]
        return movieNames;

    def makePairs(self, userRatings ):
        ratings = userRatings[1]
        (movie1, rating1) = ratings[0]
        (movie2, rating2) = ratings[1]  
        return ((movie1, movie2), (rating1, rating2))
    
    def filterBadMovies(self, userRatings):
        ratings = userRatings[1]
        (movie, rating) = ratings
        return rating > 1
    
    def filterDuplicates(self, userRatings ):
        ratings = userRatings[1]
        (movie1, rating1) = ratings[0]
        (movie2, rating2) = ratings[1]
        return movie1 < movie2


def main():
    if (len(sys.argv) == 1):
        print("Plese Provide arguments in the following order: 1. Movie Name 2. Similarity Functions")
        print("Option for similarity functions are: \n a. CosineSimilarity \n b. Jaccard similarity c. Pearson Similairy")
        sys.exit(0)
    obj = Processinfo("MovieSimilarities")
    conf = obj.connection;
    sc = SparkContext(conf = conf)
    
    print("\nLoading movie names...")
    nameDict = obj.loadMovieNames()
    
    data = sc.textFile("../ml-100k/u.data")
    
    # Map ratings to key / value pairs: user ID => movie ID, rating
    ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
    
    #new_ratings = ratings.filter(filterBadMovies)   #<--Filter out bad movies
    
    # Emit every movie rated together by the same user.
    # Self-join to find every combination.
    joinedRatings = ratings.join(ratings)
    
    # At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))
    
    # Filter out duplicate pairs
    unique_JoinedRatings = joinedRatings.filter(obj.filterDuplicates)
    
    # Now key by (movie1, movie2) pairs.
    moviePairs = unique_JoinedRatings.map(obj.makePairs)
    
    # We now have (movie1, movie2) => (rating1, rating2)
    # Now collect all ratings for each movie pair and compute similarity
    moviePairRatings = moviePairs.groupByKey()
    
    
    
    # Extract similarities for the movie we care about that are "good".
    if (len(sys.argv) > 1):
        movieID = int(sys.argv[1])
        scoreThreshold = 0.95
        coOccurenceThreshold = 100
        # We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
        # Can now compute similarities.
        moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
        
        # Save the results if desired
        #moviePairSimilarities.sortByKey()
        moviePairSimilarities.saveAsTextFile("movie-sim")
        
    
        # Filter for movies with this sim that are "good" as defined by
        # our quality thresholds above
        filteredResults = moviePairSimilarities.filter(lambda pairSim: \
            (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
            and pairSim[1][1] > coOccurenceThreshold and pairSim[1][0] > scoreThreshold)
    
        # Sort by quality score.
        results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)
    
        print("Top 10 similar movies for " + nameDict[movieID])
        for result in results:
            (sim, pair) = result
            # Display the similarity result that isn't the movie we're looking at
            similarMovieID = pair[0]
            if (similarMovieID == movieID):
                similarMovieID = pair[1]
            print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
    
    elif (len(sys.argv) > 2):
        scoreThreshold = 0.5
        coOccurenceThreshold = 100
        movieID = int(sys.argv[1])
        SimID = str(sys.argv[2])
        
        SimDict = dict(a=computeCosineSimilarity, b=JaccardSim, c=perarsonSim)
        # We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
        # Can now compute similarities.
        moviePairSimilarities = moviePairRatings.mapValues(SimDict[SimID]).cache()
        
        # Save the results if desired
        #moviePairSimilarities.sortByKey()
        moviePairSimilarities.saveAsTextFile("movie-sim")
        
        filteredResults = moviePairSimilarities.filter(lambda pairSim: \
            (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
            and pairSim[1][1] > coOccurenceThreshold and pairSim[1][0] > scoreThreshold)
    
        # Sort by quality score.
        results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)
    
        print("Top 10 similar movies for " + nameDict[movieID])
        for result in results:
            (sim, pair) = result
            # Display the similarity result that isn't the movie we're looking at
            similarMovieID = pair[0]
            if (similarMovieID == movieID):
                similarMovieID = pair[1]
            print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
if __name__=="__main__":
    main()