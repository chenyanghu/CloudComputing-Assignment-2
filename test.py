from operator import add
import sys
import re
from pyspark import SparkContext

"""Parses an entry in a line into title and outlinks."""
def parsePages(page):
        fields = page.split('\t')
        title = fields[1]
	#body = fields[3].replace("\\n", "\n")

	#outlinks = XML.loadString(body) \\ "link" \ "target"



        #title = re.findall(r'<title>(.*?)</title>', page)

        outlinks = re.findall('<target>(.*?)</target>', fields[3])

        #print("LOOK AT HERE LOOK AT HERE LOOK AT HERE LOOK AT HERE LOOK AT HERE")
        #print(fields[1])

        return title,outlinks

"Computes contributions of pages pointing to the current page"
def computeContributions(pages, pageRank):
	count = len(pages)
	for page in pages:
		yield(page, pageRank / count)

#initialize Spark Context
sc = SparkContext(appName = "WikiPageRank")

#build RDD from the input file (input as an argument)
lines = sc.textFile(sys.argv[1])

#lines = sc.textFile("/~/subset.tsv")
#lines = sc.textFile("graph2.txt")
#lines = sc.textFile("simplewiki-20150901-pages-articles-processed.xml")

#count nr of documents in a corpus
count = lines.count()

#lines.collect()

#parse file
#apply map transformation and lambda function on each line to parse a page
#links = list(K,V), where K=title, V = outgoing links
links = lines.map(lambda e: parsePages(e.encode("ascii", "ignore")))

#initialize rank for each page with 1/N, where N- total nr of pages in corpus
pageRanks = links.map(lambda link: (link[0], 1/float(count)))

#checkresults
#pageRanks.collect()

#iteratively calculate page rank
for iteration in range(10):
	#combine links and page ranks
	#links.join(pageRanks).collect()
	#calculate contributions, x[1][0] is a list of outlinks, x[1][1] is pageRank
	joined = links.join(pageRanks)
	contributions = links.join(pageRanks).flatMap(lambda x: computeContributions(x[1][0], x[1][1]))
	#contributions.collect()
	#sum all the scores for a page with a forumla with dumping factor
	pageRanks = contributions.reduceByKey(add).mapValues(lambda rank: 0.15 + 0.85 * rank)
	
#check results
#pageRanks.collect()
#save results
#pageRanks.saveAsTextFile("results.txt")

#do the sorting
pageRanksOrdered = pageRanks.takeOrdered(100, key = lambda x: -x[1])

print("LOOK AT HERE LOOK AT HERE LOOK AT HERE LOOK AT HERE LOOK AT HERE")

for(link, rank) in pageRanksOrdered:
        
	print("Title: %s Rank: %s" % (link, rank))

#pageRanksOrderedRDD = sc.parallelize(pageRanksOrdered)
#pageRanksOrderedRDD.saveAsTextFile("pageRanks_wiki")
