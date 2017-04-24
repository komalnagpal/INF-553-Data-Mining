from pyspark import SparkContext
from itertools import combinations
import itertools,sys,operator
from collections import defaultdict
class LocalSensitityHashing:
    def __init__(self, numOfBands, numOfhash):
        self.numOfBands = numOfBands
        self.numOfhash = numOfhash
        self.inputdict= None

    def readAndSplitLine(self,line):
        movie_list = line.split(",")
        return (int(str(movie_list[0])[1:]),movie_list[1:])

    def create_signature(self,line):
        user_signature = []
        for hashfunc in range(self.numOfhash):
            min_hash = float('Inf')
            for movie in line[1]:
                min_hash = min(min_hash,self.generateHash(int(movie), hashfunc))
            user_signature.append(str(min_hash))
        return (line[0],user_signature)

    def generateHash(self,row,hashnum):
        return (3*row + 13 * hashnum) % 100 

    def applyLSHBanding(self):
        candidate_pairs = set()
        sc = SparkContext(appName="hw3")
        input_file, output_file = sys.argv[1:] 
        rdd = sc.textFile(sys.argv[1])
        inputdataRDD = rdd.map(self.readAndSplitLine)
        inputRDD = inputdataRDD.map(self.create_signature)
        self.input_dict = dict(inputdataRDD.collect())
        bandSize = self.numOfhash /self.numOfBands
        counter = 0
        candidatePairs = sc.emptyRDD()
        for band in range(self.numOfBands):
            bandRDD = inputRDD.map(lambda x: (x[0],x[1][counter: counter + bandSize]))
            bandCandidatePairs = bandRDD.map(lambda userMovieDict: (tuple(userMovieDict[1]),userMovieDict[0])).groupByKey().map(lambda x: list(x[1])).flatMap(lambda x: list(combinations(x,2)))
            candidatePairs = candidatePairs.union(bandCandidatePairs)
            counter = counter + bandSize
        pairwiseJaccardSimilarityRDD = candidatePairs.distinct().map(lambda x: (x[0],x[1], self.calculateJaccardSimilarity(self.input_dict[x[0]],self.input_dict[x[1]]))).flatMap(lambda x: ((x[0],([(x[1],x[2])])),((x[1],[(x[0],x[2])])))).reduceByKey(lambda x,y: x+y).sortByKey(ascending = True)

        formattedOutputRDD = pairwiseJaccardSimilarityRDD.map(lambda x: (x[0],dict(x[1]))).map(lambda x: (x[0],sorted(x[1].items(), key=lambda k: (-k[1], k[0])))).map(lambda x: (x[0],x[1][:5])).map(lambda x: ''.join(s for s in ['U'+ str(x[0]),':',','.join('U'+str(movieName)for movieName in sorted([a[0] for a in x[1]]))])).collect()

        outfile = open(output_file,"w")
        for line in formattedOutputRDD:
            outfile.write(line)
            outfile.write("\n")
        outfile.close()
        
    def calculateJaccardSimilarity(self, vec1, vec2):
        set1 = set(vec1)
        set2 = set(vec2)
        return float(len(set1 & set2)) / len(set1 | set2)
        

if __name__ == "__main__":
    lsh = LocalSensitityHashing(5,20)
    lsh.applyLSHBanding()
