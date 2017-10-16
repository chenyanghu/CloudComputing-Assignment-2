from operator import add
import sys
import re
from pyspark import SparkContext

"""Parses an entry in a line into title and outlinks."""
def parsePages(page):
        fields = page.split('\t')
        
        #fields[1] title
        #fields[3] XML
        title = fields[1]

        outlinks = re.findall('<target>(.*?)</target>', fields[3])


        return title,outlinks

"Computes contributions of pages pointing to the current page"
def computeContributions(pages, pageRank):
	count = len(pages)
	for page in pages:
		yield(page, pageRank / count)

sc = SparkContext(appName = "WikiPageRank")

lines = sc.textFile(sys.argv[1])

count = lines.count()

links = lines.map(lambda e: parsePages(e.encode("ascii", "ignore")))

#initialize rank for each page with 1/N, where N- total nr of pages in corpus
pageRanks = links.map(lambda link: (link[0], 1/float(count)))

#iteratively calculate page rank
for iteration in range(10):
	#x[1][0] is a list of outlinks, x[1][1] is pageRank
	joined = links.join(pageRanks)
	contributions = links.join(pageRanks).flatMap(lambda x: computeContributions(x[1][0], x[1][1]))
	#sum all the scores for a page with a forumla with dumping factor
	pageRanks = contributions.reduceByKey(add).mapValues(lambda rank: 0.15 + 0.85 * rank)

#do the sorting
pageRanksOrdered = pageRanks.takeOrdered(100, key = lambda x: -x[1])

print("LOOK AT HERE LOOK AT HERE LOOK AT HERE LOOK AT HERE LOOK AT HERE")

for(link, rank) in pageRanksOrdered:
        
	print("Title: %s Rank: %s" % (link, rank))

