from scipy.spatial.distance import cosine
from pyechonest import config
from pyechonest.artist import Artist
from pyechonest.util import EchoNestAPIError,EchoNestIOError
from httplib import BadStatusLine
from urllib2 import unquote
import socket
import time
import MySQLdb
import itertools
import math
from scipy import sparse
import numpy as np

# to ensure proper handling of keyboard interrupts
import win32api
import thread
def handler(sig, hook=thread.interrupt_main):
	hook()
	return 1
win32api.SetConsoleCtrlHandler(handler, 1)


def sparse_loader(filename):
	npz = np.load(filename)
	return sparse.csr_matrix((npz['data'], npz['indices'], npz['indptr']), shape=npz['shape'])

"""
Retrives terms from echonest API, given an artist names.
Result is converted to dict in the form
	{term_1:weight_1,term_2:weight_2,...,term_n:weight_n}
and returned by function
"""
def echonest_terms(artist):
	while True:
		try:
			return {i['name']:i['weight'] for i in Artist(artist).terms}
		except EchoNestIOError:
			time.sleep(5)
		except EchoNestAPIError,error:
			http_status = error.http_status
			# If the artist is not found, return empty dict
			# EchoNestAPIError: (u'Echo Nest API Error 5: The Identifier specified does not exist [HTTP 200]',)
			if http_status in [200,400]:
				return {}
			# If we've exceeded the rate limit, wait 5 seconds and then try again
			# EchoNestAPIError: (u'Echo Nest API Error 3: 3|You are limited to 120 accesses every minute. You might be eligible for a rate limit increase, go to http://developer.echonest.com/account/upgrade [HTTP 429]',)
			elif http_status in [429,500,503]:
				time.sleep(5)
			# Any other error, raise the exception

			else:
				print artist
				raise error
		except socket.error:
			print artist
			time.sleep(5)
		except BadStatusLine:
			print artist
			time.sleep(5)


"""
Given an artist ID and name, retrive terms for that artist.
First tries pulling from database. If not present, makes call to Echonest API
"""
def get_terms(artist_id,artist_name):
	cursor.execute("select terms from echonest_terms where artist_id=%s",(artist_id,))
	result = cursor.fetchone()
	if result:
		return eval(result[0])
	else:
		terms = echonest_terms(artist_name)
		cursor.execute("insert into echonest_terms (artist_id,terms) values (%s,%s);", (artist_id,str(terms)))
		db.commit()
		return terms

"""
Calculate cosine similarity between term vectors for two artists.
If term vector is empty for an artist, return NULL
"""
def term_cosine_sim(artist1_id,artist1_name,artist2_id,artist2_name):
	a1Terms = get_terms(artist1_id,artist1_name)
	a2Terms = get_terms(artist2_id,artist2_name)
	if (a1Terms == {}) or (a2Terms == {}):
		return -1
	union = set(a1Terms.keys()) | set(a2Terms.keys())
	a1vec = [a1Terms.get(i,0.0) for i in union]
	a2vec = [a2Terms.get(i,0.0) for i in union]
	return 1-cosine(a1vec,a2vec)

"""
Get cosine similariy for a pair of artists on echonest term data.
First tries pulling from database, otherwise calls term_cosine_sim
"""
def echonest_term_sim(artist1_id,artist1_name,artist2_id,artist2_name):
	if artist1_id==artist2_id:
		return 1.0
	if artist1_id>artist2_id:
		artist1_id,artist2_id = artist2_id,artist1_id
		artist1_name,artist2_name = artist2_name,artist1_name
	cursor.execute("select sim_echonest_terms_cosine from sim_echonest_terms where artist_id_1=%s and artist_id_2=%s",(artist1_id,artist2_id))
	result = cursor.fetchone()
	if result:
		return result[0]
	else:
		sim = term_cosine_sim(artist1_id,artist1_name,artist2_id,artist2_name)
		cursor.execute("insert into sim_echonest_terms (artist_id_1,artist_id_2,sim_echonest_terms_cosine) values (%s,%s,%s);", (artist1_id,artist2_id,sim))
		db.commit()
		return sim

"""
def user_listening_cosine_sim(artist1_id,artist2_id):#,user_artist_matrix,artist_dict):
	a1vec = user_artist_matrix[artist_dict[artist1_id]].toarray()#todense().A1
	a2vec = user_artist_matrix[artist_dict[artist2_id]].toarray()#todense().A1
	return 1-cosine(a1vec,a2vec)
"""
def user_listening_cosine_sim(a1vec,a2vec):#,user_artist_matrix,artist_dict):
	#a1vec = user_artist_matrix[artist_dict[artist1_id]].toarray()#todense().A1
	#a2vec = user_artist_matrix[artist_dict[artist2_id]].toarray()#todense().A1
	return 1-cosine(a1vec.toarray(),a2vec.toarray())

"""
Calculate cosine similairty of artists on user co-listening data.
"""

def user_listening_sim(artist1_id,artist2_id,min_listeners=25):#,mat,artist_dict):
	if artist1_id==artist2_id:
		return 1.0
	if artist1_id>artist2_id:
		artist1_id,artist2_id = artist2_id,artist1_id
		#artist1_name,artist2_name = artist2_name,artist1_name
	cursor.execute("select sim_user_listening_cosine from sim_listening where artist_id_1=%s and artist_id_2=%s",(artist1_id,artist2_id))
	result = cursor.fetchone()

	# checking number of listeners here, then passing things along as needed
	a1vec = user_artist_matrix[artist_dict[artist1_id]]
	a2vec = user_artist_matrix[artist_dict[artist2_id]]
	if (len(a1vec.indices)<min_listeners) or (len(a2vec.indices)<min_listeners):
		sim = -1
	else:
		sim = None

	### TEMP TIL WE FIGURE OUT A BETTER METHOD
	if result:
		#return result[0]
		if not sim:
			sim=result[0]
	else:
		#sim = user_listening_cosine_sim(artist1_id,artist2_id)
		cosine_sim = user_listening_cosine_sim(a1vec,a2vec)
		if not sim:
			sim = cosine_sim
		cursor.execute("insert into sim_listening (artist_id_1,artist_id_2,sim_user_listening_cosine) values (%s,%s,%s);", (artist1_id,artist2_id,cosine_sim))
		db.commit()
		#return sim
	return sim



#Processes a listening time series retrieved from database, represented as a tuple:
#	((artist_id_1,artist_name_1),(artist_id_2,artist_name_2),...,(artist_id_b,artist_name_n))
#Returns list of artist similarities of length n-1, where n is the length of the time series.

def process_artist_seq(seq,mode='echonest_term',verbose=False):#,mat=None,artist_dict=None):
	#start = time.time()
	sims = []
	last_id = seq[0][0]
	last_name = seq[0][1].replace('+',' ')
	#for i,(artist_id,artist_name) in enumerate(seq[1:]):
	for artist_id,artist_name,ts in seq[1:]:
		artist_name = unquote(artist_name).replace('+',' ')
		if artist_id == last_id:
			sim = 1.0
		else:
			if mode == 'echonest_term':
				sim = echonest_term_sim(artist_id,artist_name,last_id,last_name)
			elif mode == 'user_listening_vec':
				sim = user_listening_sim(artist_id,last_id)#,mat,artist_dict)
		sims.append(sim)
		if verbose:
			print artist_id,artist_name,last_id,last_name,sim
		last_id = artist_id
		last_name = artist_name
	return sims



### CURRENT FUNCTIONAL SIM_SEQ CODE

config.ECHO_NEST_API_KEY=open('api.key').read()
mysql_user,mysql_pass = [line.strip() for line in open('mysql.key')]
db = MySQLdb.connect(host="127.0.0.1", user=mysql_user, passwd=mysql_pass,db="analysis_lastfm")
cursor = db.cursor()

if False: # only use this if we're picking up from an incomplete file
	fi = 'E:/BTSync/Research.Archive/LastFM/MusicPatches/simSeqs_backup2.tsv'
	out = open('E:/BTSync/Research.Archive/LastFM/MusicPatches/simSeqs.tsv','w')
	infile = open(fi,'r')
	done = {}
	last = None
	for i,line in enumerate(infile):
		if last:
			out.write(last)
			done[int(last.strip().split('\t')[0])] = None
		last = line
	infile.close()
	out.close()
done = {}


# We want the same random order from before, I guess
#n = cursor.execute("select user_id from lastfm_users where sample_playcount>=100 order by rand();")
#users = cursor.fetchall()
users = [int(line.strip().split('\t')[0]) for line in open('E:/BTSync/Research.Archive/LastFM/MusicPatches/simSeqs_terms.tsv')]
nUsers = len(users)

user_artist_matrix = sparse_loader('E:/BTSync/Research.Archive/LastFM/MusicPatches/user_artist_listening_matrix.npz')
artist_dict = {}
for line in open('artist_indices'):
	line = map(int,line.strip().split('\t'))
	artist_dict[line[1]] = line[0]

out = open('E:/BTSync/Research.Archive/LastFM/MusicPatches/simSeqs_listening.tsv','a')


for i,user in enumerate(users):
	#user = user[0]
	try:
		if user in done:
			continue
			print 'user %s (already done)' % (user,), '%s / %s' % (i+1,n), '%0.2f percent complete' % (100*((i+1.)/n)), time.strftime("%d %b %Y %H:%M:%S", time.localtime())
		scrobbles = cursor.execute("select s.artist_id,i.artist,s.scrobble_time from lastfm_scrobbles s join lastfm_itemlist i on s.artist_id=i.item_id where \
					user_id = %s order by scrobble_time asc", (user,))
		result = cursor.fetchall()
		sims = process_artist_seq(result,mode='user_listening_vec')#,mat,artist_dict)
		out.write('\t'.join(map(str,[user]+sims))+'\n')
		print 'user %s (%s scrobbles)' % (user,scrobbles), '%s / %s' % (i+1,nUsers), '%0.2f percent complete' % (100*((i+1.)/nUsers)), time.strftime("%d %b %Y %H:%M:%S", time.localtime())
	except KeyboardInterrupt:
		print 'Stopping...'
		break

out.close()

cursor.close()
db.close()

""" # BLOCK TO GET MISSING ARTIST terms

n = cursor.execute("select i.item_id,i.artist from echonest_terms e right outer join lastfm_itemlist i on e.artist_id=i.item_id where i.item_type=0 and e.terms is null;")
result = cursor.fetchall()

for i,(artist_id,artist_name) in enumerate(result):
	artist_name = unquote(artist_name).replace('+',' ')
	print artist_id,artist_name
	print "%s / %s" % (i+1,n)
	terms = get_terms(artist_id,artist_name)
db.close()

"""