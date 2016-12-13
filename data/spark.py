from pyspark import SparkContext
from itertools import combinations
from random import shuffle

def getCombinations(line):
	return [ (line[0], (x[0], x[1])) for x in combinations(line[1],2)]

def getUniqueUserCount(line):
	uniqueSet = []
	for elem in line[1]:
		if elem not in uniqueSet:
			uniqueSet.append(elem)
	return (line[0], len(uniqueSet))

sc = SparkContext("spark://spark-master:7077", "PopularItems")

data = sc.textFile("/tmp/data/clickdata2.tsv", 2).distinct()     # each worker loads a piece of the data file

pairs = data.map(lambda line: line.split("\t"))   # tell each worker to split each line of it's partition
lists = pairs.groupByKey()
click_pairs = lists.flatMap(getCombinations) \
				.filter(lambda line: len(line[1]) > 1) \
				.filter(lambda line: line[1][0] != line[1][1])
coclicks = click_pairs.map(lambda line: (line[1], line[0]))
coclicks_merged = coclicks.reduceByKey(lambda x, y: x + y) \
				.map(getUniqueUserCount) \
				.filter(lambda line: line[1] > 2)


output = coclicks_merged.collect() # bring the data back to the master node so we can print it out
for pair, count in output:
	print(pair[0]  + ',' + pair[1] + ':', end='')
	print(count)

sc.stop()
