import itertools
import multiprocessing as mp
import MySQLdb
import MySQLdb.cursors
import time
import math


# dummy similarity function
def similarity(tup):
	a,b=tup
	time.sleep(0.25)
	return a[0]+b[0]

if __name__ == '__main__':

	db = MySQLdb.connect(host="127.0.0.1", user='root', passwd='root',db="analysis_lastfm")#,cursorclass = MySQLdb.cursors.SSCursor)
	cursor = db.cursor()

	t1 = time.time()

	## Step 0: Query the database. Don't do fetchall if you can avoid it, this is
	## faster if cursor stays an iterator.
	n = 100
	cursor.execute("select * from lastfm_groups limit %s",(n,))

	## Step 1: Make the input list into a list of pairs using itertools
	a, b = itertools.tee(cursor)
	_ = b.next()
	# Returns 2-tuples like (cursor[0], cursor[1]), ... until cursor is exhausted
	tuples = itertools.izip(a, b)

	## Step 2: Assume similarity(.,.) is a function, apply it to each 2-tuple
	# Manually tune the number of processes. Start with the number of (virtual) CPUs
	nProc = mp.cpu_count()
	chunksize = int(math.ceil(n / float(nProc)))
	print nProc,chunksize
	pool = mp.Pool(processes=nProc)
	# Manually tune the chunksize here. It's the number of tuples read at one time
	results1 = [_ for _ in pool.imap(func=similarity, iterable=tuples, chunksize=chunksize)] # 8192
	print len(results1)
	
	print time.time()-t1

	t2 = time.time()
	cursor.execute("select * from lastfm_groups limit %s",(n,))
	results2 = []
	last = cursor.fetchone()[0]
	for a,b in cursor:
		results2.append(last+a)
		time.sleep(0.25)
		last = a
	print len(results2)
	print time.time()-t2
	# print results1==results2





	# Results should respect the input order.
	#print results
	db.close()	