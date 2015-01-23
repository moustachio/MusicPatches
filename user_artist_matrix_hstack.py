import MySQLdb
import numpy as np
from scipy import sparse
import os
import time

mysql_user,mysql_pass = [line.strip() for line in open('mysql.key')]
db = MySQLdb.connect(host="127.0.0.1", user=mysql_user, passwd=mysql_pass,db="analysis_lastfm")
dbSS = MySQLdb.connect(host="127.0.0.1", user="root", passwd="root",db="analysis_lastfm",cursorclass=MySQLdb.cursors.SSCursor) 
cursor = db.cursor()
cursorSS = dbSS.cursor()

def sparse_saver(mat,filename):
	np.savez_compressed(filename,data=mat.data,indices=mat.indices,indptr=mat.indptr,shape=mat.shape)

def sparse_loader(filename):
	npz = np.load(filename)
	return sparse.csr_matrix((npz['data'], npz['indices'], npz['indptr']), shape=npz['shape'])


### Get the artists we know we care about
nArtists = 0
cursorSS.execute("select artist_id from echonest_terms;")
out = open('artist_indices','w')
artistID_idx_dict = {}
for i,row in enumerate(cursorSS):
	artistID = row[0]
	artistID_idx_dict[artistID] = i
	out.write('\t'.join(map(str,[i,artistID]))+'\n')
	nArtists += 1
out.close()
print "artist IDs retrieved (%s)" % time.strftime("%d %b %Y %H:%M:%S", time.localtime())
print "%s total artists" % nArtists

### Generate user_id / index dict
nUsers = cursor.execute("select user_id,sample_playcount from lastfm_users where sample_playcount>=100;")
out = open('user_indices','w')
userID_idx_dict = {}
userIDs = []
for i,(uid,playcount) in enumerate(cursor.fetchall()):
	userIDs.append(uid)
	userID_idx_dict[uid] = (i,float(playcount))
	out.write('\t'.join(map(str,[i,uid,playcount]))+'\n')
print "user data populated (%s)" % time.strftime("%d %b %Y %H:%M:%S", time.localtime())
print "%s total users" % nUsers
out.close()


### initializtion

chunksize = 100
#nArtists = 4475696
mat = np.zeros((nArtists,chunksize))
chunk_i=0
chunk=0
ddir = 'E:/chunks/'

### Main loop

for i,user_id in enumerate(userIDs):
	print time.strftime("%d %b %Y %H:%M:%S", time.localtime())
	print 'Current user ID: %s (%s/%s, %0.2f percent)' % (user_id, i+1, nUsers, (100*(i+1.)/nUsers))

	if chunk_i%chunksize==0 and chunk_i>0:
		sparse_mat = sparse.csr_matrix(mat)
		print 'saving chunk %s' % chunk
		fi = ddir+'chunk%03d' % chunk
		sparse_saver(sparse_mat,fi)
		mat = np.empty((nArtists,chunksize))
		chunk+=1
		chunk_i = 0

	cursor.execute("select artist_id,count(*) as freq from lastfm_scrobbles where user_id=%s group by artist_id;",(user_id,))
	for artist_id,freq in cursor.fetchall():
		idx = artistID_idx_dict[artist_id]
		total_playcount = userID_idx_dict[user_id][1]
		mat[idx,chunk_i] = freq / total_playcount
	chunk_i += 1

db.close()
dbSS.close()
out.close()

# we initialized each chunk to be chunksize rows, so we have to trim the last one down.
#trimmed = mat[~np.all(mat==0,axis=1)]
trimmed = mat[:,mat.sum(0)>0]
sparse_mat = sparse.csr_matrix(trimmed)
print 'saving chunk %s' % chunk
fi = ddir+'chunk%03d' % chunk
sparse_saver(sparse_mat,fi)

# Now we concatenate all the chunks together into one big sparse matrix.
prefix = ddir
files = [f for f in os.listdir(prefix) if '.npz' in f]
# This line is generating an error, see https://github.com/scipy/scipy/issues/3572
#data = sparse.csr_matrix((nArtists,0))
# so instead we load the first file, the concatenate the rest to that
data = sparse_loader(prefix+files[0])
for fi in files[1:]:
	print fi
	data = sparse.hstack([data,sparse_loader(prefix+fi)],format='csr')
sparse_saver(data,'E:/BTSync/Research.Archive/LastFM/MusicPatches/user_artist_listening_matrix')

