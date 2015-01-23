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

### Generate user_id / index dict
nUsers = cursor.execute("select user_id,sample_playcount from lastfm_users where sample_playcount>=100;")
out = open('user_indcies','w')
userID_idx_dict = {}
for i,(uid,playcount) in enumerate(cursor.fetchall()):
	userID_idx_dict[uid] = (i,float(playcount))
	out.write('\t'.join(map(str,[i,uid,playcount]))+'\n')
print "user data populated (%s)" % time.strftime("%d %b %Y %H:%M:%S", time.localtime())

### Get the artists we know we care about
cursorSS.execute("select artist_id from echonest_terms;")
#print "artist IDs retrieved (%s)" % time.strftime("%d %b %Y %H:%M:%S", time.localtime())
#print "%s total artists" % nArtists

### initializtion

chunksize = 10000
nArtists = 4475696
mat = np.zeros((chunksize,nUsers))
chunk_i=0
chunk=0
ddir = 'E:/chunks/'

### Main loop

artist_idx_log = open('artist_indices','w')
for i,row in enumerate(cursorSS):
	print time.strftime("%d %b %Y %H:%M:%S", time.localtime())
	print 'Current artist ID: %s (%s/%s, %0.2f percent)' % (row[0], i+1, nArtists, (100*(i+1.)/nArtists))
	artist_idx_log.write(str(i)+'\t'+str(row[0])+'\n')

	if chunk_i%chunksize==0 and chunk_i>0:
		sparse_mat = sparse.csr_matrix(mat)
		print 'saving chunk %s' % chunk
		fi = ddir+'chunk%03d' % chunk
		sparse_saver(sparse_mat,fi)
		mat = np.empty((chunksize,nUsers))
		chunk+=1
		chunk_i = 0

	cursor.execute("select user_id,count(*) as freq from lastfm_scrobbles where artist_id=%s group by user_id;",row)
	for user_id,freq in cursor.fetchall():
		try:
			idx,total_playcount = userID_idx_dict[user_id]
		except KeyError:
			continue
		mat[chunk_i,idx] = freq / total_playcount

	chunk_i += 1

db.close()
out.close()

# we initialized each chunk to be chunksize rows, so we have to trim the last one down.
trimmed = mat[~np.all(mat==0,axis=1)]
sparse_mat = sparse.csr_matrix(trimmed)
print 'saving chunk %s' % chunk
fi = drive+'chunk%03d' % chunk
sparse_saver(sparse_mat,fi)

# Now we concatenate all the chunks together into one big sparse matrix.
prefix = ddir
files = [f for f in os.listdir(prefix) if '.npz' in f]
data = sparse.csr_matrix((0,nUsers))
for fi in files:
	data = sparse.vstack([data,sparse_loader(prefix+fi)])
sparse_saver(data,'E:/BTSync/Research.Archive/LastFM/MusicPatches/user_artist_listening_matrix')
