from scipy.stats.stats import pearsonr
from sklearn.metrics import jaccard_similarity_score as JS
from math import sqrt

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)

def perarsonSim(ratingPairs):
    numPairs = len(ratingPairs)
    ratingX = [int(x) for x, y in ratingPairs]
    ratingY = [int(y) for x, y in ratingPairs]
    score = pearsonr(ratingX, ratingY)
    return (score[0], numPairs)

def JaccardSim(ratingPairs):
    numPairs = len(ratingPairs)
    ratingX = [int(x) for x, y in ratingPairs]
    ratingY = [int(y) for x, y in ratingPairs]
    score = JS(ratingX, ratingY)
    return (score, numPairs)