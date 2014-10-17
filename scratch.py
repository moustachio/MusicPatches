from scipy.spatial.distance import cosine
from pyechonest import config
from pyechonest.artist import Artist
from pyechonest.util import EchoNestAPIError
from urllib2 import unquote
import time
import MySQLdb


config.ECHO_NEST_API_KEY=open('api.key').read()
mysql_user,mysql_pass = [line.strip() for line in open('mysql.key')]

db = MySQLdb.connect(host="127.0.0.1", user=mysql_user, passwd=mysql_pass,db="analysis_lastfm")
cursor = db.cursor()


### This is just for debugging...returns a sampe set of data (my time-ordered listening history)
### But the format should stay the change. Code below assumes input is in the form of tuples:
###       ((artist_id_1,artist_name_1),(artist_id_2,artist_name_2),...,(artist_id_b,artist_name_n))

#cursor.execute("select s.artist_id,i.artist from lastfm_scrobbles s join lastfm_itemlist i on s.artist_id=i.item_id where \
#				user_id = (select user_id from lastfm_users where user_name='AxelStreichen') order by scrobble_time asc")
#testData = cursor.fetchall()


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
		except EchoNestAPIError,error:
			http_status = error.http_status
			# If the artist is not found, return empty dict
			# EchoNestAPIError: (u'Echo Nest API Error 5: The Identifier specified does not exist [HTTP 200]',)
			if http_status == 200:
				return {}
			# If we've exceeded the rate limit, wait 5 seconds and then try again
			# EchoNestAPIError: (u'Echo Nest API Error 3: 3|You are limited to 120 accesses every minute. You might be eligible for a rate limit increase, go to http://developer.echonest.com/account/upgrade [HTTP 429]',)
			elif http_status == 429:
				time.sleep(5)
			# Any other error, raise the exception
			else:
				raise error


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
	if (a1Terms == {}) or (a2Terms =={}):
		return None
	union = set(a1Terms.keys()) | set(a2Terms.keys())
	a1vec = [a1Terms.get(i,0.0) for i in union]
	a2vec = [a2Terms.get(i,0.0) for i in union]
	return 1-cosine(a1vec,a2vec)

"""
Get cosine similariy for a pair of artists.
First tries pulling from database, otherwise calls term_cosine_sim
"""
def get_sim(artist1_id,artist1_name,artist2_id,artist2_name):
	if artist1_id>artist2_id:
		artist1_id,artist2_id = artist2_id,artist1_id
		artist1_name,artist2_name = artist2_name,artist1_name
	cursor.execute("select sim_terms_cosine from echonest_similarity where artist_id_1=%s and artist_id_2=%s",(artist1_id,artist2_id))
	result = cursor.fetchone()
	if result:
		return result[0]
	else:
		sim = term_cosine_sim(artist1_id,artist1_name,artist2_id,artist2_name)
		cursor.execute("insert into echonest_similarity (artist_id_1,artist_id_2,sim_terms_cosine) values (%s,%s,%s);", (artist1_id,artist2_id,sim))
		db.commit()
		return sim

"""
Processes a listening time series retrieved from database, represented as a tuple:
	((artist_id_1,artist_name_1),(artist_id_2,artist_name_2),...,(artist_id_b,artist_name_n))
Returns list of artist similarities of length n-1, where n is the length of the time series.
"""
def process_artist_seq(seq,verbose=False):
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
			# THIS TRY/EXCEPT BLOCK IS FOR DEBUGGING ONLY
			#try:
			sim = get_sim(artist_id,artist_name,last_id,last_name)
			#except: 
			#	print artist_id,artist_name,last_id,last_name
			#	return artist_id,artist_name,last_id,last_name
		sims.append(sim)
		if verbose:
			print artist_id,artist_name,last_id,last_name,sim
		last_id = artist_id
		last_name = artist_name
	return sims

"""
Simple, temporary function to handle writing output to file
"""

def output_handler(user,listening_seq,sim_seq,fi,filemode):
	out = open(fi,filemode)
	for (artist_id,artist_name,ts),sim in zip(listening_seq,[None]+sim_seq):
		out.write('\t'.join(map(str,[user,artist_id,artist_name,ts,sim]))+'\n')
	out.close()


###
# Block to generate sample data
###
cursor.execute("select user_id from lastfm_users where sample_playcount>=10000 limit 25;")
users = cursor.fetchall()
for i,user in enumerate(users):
	print i,user[0]
	cursor.execute("select s.artist_id,i.artist,s.scrobble_time from lastfm_scrobbles s join lastfm_itemlist i on s.artist_id=i.item_id where \
				user_id = %s order by scrobble_time asc", (user[0],))
	result = cursor.fetchall()
	sims = process_artist_seq(result)
	fi = '25_sample_users_for_ke.tsv'
	if i==0:
		output_handler(user[0],result,sims,fi,'w')
	else:
		output_handler(user[0],result,sims,fi,'a')

cursor.close()
db.close()



