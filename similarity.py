from scipy.stats.stats import pearsonr
from sklearn.metrics import jaccard_similarity_score as JS
from math import sqrt
import functools

#Decorator to check if score is numeric or not
def Check_Result(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        result = function(*args, **kwargs);
        score, numPairs = result
        if str(score).replace(".","",1).isdigit():
            pass;
        else:
            return (0.0, numPairs);
        return result
    return wrapper
            

@Check_Result
def computeCosineSimilarity(ratingPairs: dict(type = 'RDD ROW', help = 'A single Row of RDD in the form of (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...')) -> 'computed score and total number of pairs':
    numPairs = len(ratingPairs)
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)
@Check_Result
def perarsonSim(ratingPairs: dict(type = 'RDD ROW', help = 'A single Row of RDD in the form of (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...')) -> 'computed score and total number of pairs':
    numPairs = len(ratingPairs)
    ratingX = [int(x) for x, y in ratingPairs]
    ratingY = [int(y) for x, y in ratingPairs]
    score = pearsonr(ratingX, ratingY)
    return (score[0], numPairs)
    
@Check_Result
def JaccardSim(ratingPairs: dict(type = 'RDD ROW', help = 'A single Row of RDD in the form of (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...')) -> 'computed score and total number of pairs':
    numPairs = len(ratingPairs)
    ratingX = [int(x) for x, y in ratingPairs]
    ratingY = [int(y) for x, y in ratingPairs]
    score = JS(ratingX, ratingY)
    return (score, numPairs)