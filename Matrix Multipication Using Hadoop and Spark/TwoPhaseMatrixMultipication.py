from pyspark import SparkContext
import itertools
import sys


def reducer(x):
    matrixA = []
    matrixB = []
    combinations = []
    result = []
    for val in x:
        if "A" == val[0]:
            matrixA.append(val)
        else:
            matrixB.append(val)
    for element1 in matrixA:
        for element2 in matrixB:
            result.append(((element1[1],element2[1]),int(element1[2]) * int(element2[2])))
    return result


if __name__ == "__main__":
    matrixA, matrixB, output_file_path = sys.argv[1:]
    sc = SparkContext(appName="hw1")
    matrixA_rdd = sc.textFile(matrixA)
    matrixB_rdd = sc.textFile(matrixB)
    A = matrixA_rdd.map(lambda x: x.split(',')).map(
        lambda y: ((y[1], ("A", y[0], y[2])))).collect()
    B = matrixB_rdd.map(lambda x: x.split(',')).map(
        lambda y: ((y[0], ("B", y[1], y[2])))).collect()
    input_data_rdd = sc.parallelize(A+B)
    output = input_data_rdd.groupByKey().flatMap(
        lambda x: reducer(x[1])).reduceByKey(lambda x, y: x + y).map(
        lambda a: ",".join([a[0][0], a[0][1]]) + '\t' + str(
            a[1])).collect()
    output_file = open(output_file_path,'w')
    for item in output:
        output_file.write("%s\n" %item)
