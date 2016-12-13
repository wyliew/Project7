from pyspark import SparkContext
from itertools import combinations

def getCombinations(line):
	return [ (x[0], x[1]) for x in combinations(line[1],2)]

sc = SparkContext("spark://spark-master:7077", "PopularItems")

data = sc.textFile("/tmp/data/clickdata.tsv", 2).distinct()     # each worker loads a piece of the data file

pairs = data.map(lambda line: line.split("\t"))   # tell each worker to split each line of it's partition
lists = pairs.groupByKey()
click_pairs = lists.flatMap(getCombinations)
#pages = pairs.map(lambda pair: (pair[1], 1))      # re-layout the data to ignore the user id
#count = pages.reduceByKey(lambda x,y: x+y)        # shuffle the data so that each key is only on one worker
                                                  # and then reduce all the values by adding them together

output = click_pairs.collect()                          # bring the data back to the master node so we can print it out
for page_id, l in output:
	print(page_id + ':', end='')
	for elem in l:
		print(elem, end=',')
	print()

sc.stop()
