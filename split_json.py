# folder with csv tweets to json

import json, glob, os, logging, argparse, time, ast
import pandas as pd
import multiprocessing as mp


logger = logging.getLogger(__name__)

def split(input_file):
	'''
	Read all csv files of tweets for each
	'''


	with open(input_file) as json_file:
		json_data = json.load(json_file)
		#json_data = ast.literal_eval(json.dumps(json.loads(json_file)))
		#print(json_data)
		save_json(json_data, args.output_dir, os.path.split(input_file)[1])

	output_file = os.path.join(args.output_dir, os.path.split(input_file)[1])
	df = pd.read_json(output_file, orient='index')
	df = df.sample(frac=1).T

#print(df.head())

	df = filter_df(df)
#print(df.head())
	
	return df
	

def filter_df(df):
	publicProfiles = df["is_private"] == False
	oldProfiles = df["has_anonymous_profile_picture"]== False
	return df[publicProfiles & oldProfiles]
	'''
	size_of_chunks =  5000
	index_for_chunks = list(range(0, index.max(), size_of_chunks))
	index_for_chunks.extend([index.max()+1])

	for i in range(len(index_for_chunks)-1):
		df1 = df.iloc[index_for_chunks[i]:index_for_chunks[i+1]]
		filename = 'instagram_followers'+str(i)+'.json'
		save_json(df1, output_dir, filename)
		logger.info('Saved profiles {} to {} in {}'.
					format(index_for_chunks[i],index_for_chunks[i+1],filename))
	'''
	

def save_json(j, output_dir, filename='records_grouped.json', ):
	output_file = os.path.join(output_dir, filename)
	with open(output_file, 'w') as outfile:
		outfile.write(json.dumps(j, indent=4, sort_keys=False))
	
	logger.info('Saved json file in {}'.format(output_file))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
	
	# construct the argument parser and parse the arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input_dir", default = 'output/', type=str, action='store', dest='input_dir', help="Path to the twitter histories")
    ap.add_argument("-o", "--output_dir", default = 'output_new/', type=str, action='store', dest='output_dir', help="Path to the outputs generated")
    args = ap.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    ig_users = os.path.join(args.output_dir, 'ig_users.txt')
    df_usernames = []
    followerLists = glob.glob(os.path.join(args.input_dir, '*.json'))
    
#print(followerLists)
    
    if os.path.exists(args.input_dir):
        for followerList in followerLists:
            print('Reading '+followerList+' . . . ')
            df_filtered = split(followerList)
            df_usernames.extend(df_filtered['username'].values)
            
            df_usernames1 = pd.DataFrame({'username':df_usernames})
            df_usernames1.to_csv(ig_users, index=False, header=False)
            print('Filtered profiles: ', len(df_usernames))
    else:
        logging.info('File does not exist.')
