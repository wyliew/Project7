from pyspark import SparkContext
from itertools import combinations

def getCombinations(line):
	return [ (x[0], x[1]) for x in combinations(line[1],2)]

def getUniqueUserCount(line):
	uniqueSet = []
	for elem in line[1]:
		if elem not in uniqueSet:
			uniqueSet.append(elem)
	return (line[1], len(uniqueSet))

sc = SparkContext("spark://spark-master:7077", "PopularItems")

data = sc.textFile("/tmp/data/clickdata.tsv", 2).distinct()     # each worker loads a piece of the data file

pairs = data.map(lambda line: line.split("\t"))   # tell each worker to split each line of it's partition
lists = pairs.groupByKey()
click_pairs = lists.flatMap(getCombinations) \
				.filter(lambda line: len(line[1]) > 1) \
				.filter(lambda line: line[1][0] != line[1][1])
coclicks = click_pairs.map(lambda line: (line[1], line[0]))
coclicks_merged = coclicks.reduceByKey(lambda x, y: x + y) \
				.map(getUniqueUserCount) \
				.filter(lambda line: line[1] > 2)

#pages = pairs.map(lambda pair: (pair[1], 1))      # re-layout the data to ignore the user id
#count = pages.reduceByKey(lambda x,y: x+y)        # shuffle the data so that each key is only on one worker
                                                  # and then reduce all the values by adding them together

output = coclicks_merged.collect()                          # bring the data back to the master node so we can print it out
for pair, count in output:
	print(pair[0]  + ',' + pair[1] + ':', end='')
	print(count)

sc.stop()
