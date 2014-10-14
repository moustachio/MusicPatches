from pyechonest import config,artist
config.ECHO_NEST_API_KEY=open('api.key').read()

def calc_sim(artist1,artist2):



def process_artist_seq(seq):
	sims = []
	last = seq[0]
	for artist in seq[1:]:
		if artist == last:
			sims.append(1.0)
		else:
			sims.append(calc_sim(artist,last))

