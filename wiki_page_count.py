#!/usr/bin/env python2
###########################################################################
# Program: wiki_page_count.py
# Date: 7/5/2018
# Author: Hossein Parsa 
###########################################################################

import sys, os, requests, errno, warnings
import pandas as pd
from datetime import datetime as dt

###########################################################################
#                       Function usage
###########################################################################

def usage(errMsg):
	print 'Error!', errMsg
	print 'Syntax:'
	print '         python precess_wiki_page_count.py date hour'
	print 'where: date=yyyy-mm-dd and should be a valid date and hour=HH24 between 0 and 23'
	print 'e.g python wiki_page_count.py 2012-01-01 00'
	sys.exit()

###########################################################################
#                       Make directory
###########################################################################

def mkdir_p(path):
	try:
		os.makedirs(path)
	except OSError as e: 
		if e.errno == errno.EEXIST and os.path.isdir(path):
			pass
		else:
			usage('Failed to create directory '+path+'!\n'+e)

###########################################################################
#		        END OF FUNCTIONS 
###########################################################################
warnings.simplefilter(action='ignore', category=FutureWarning)
args=sys.argv

#read args
if len(args)==3:
	pageDate=args[1]
	pageHour=args[2]
else: usage('Incorrect number of arguments.')

#extract and validate values
try:
	pageDateList=pageDate.split('-')
	pageYear, pageMonth, pageDay=pageDateList[0], pageDateList[1], pageDateList[2]
	pageDate=dt(int(pageYear),int(pageMonth), int(pageDay))
	if int(pageHour) not in range(24):
		usage('Incorrect hour value.')
except Exception as e:
	usage('Invalid date. '+str(e))

pageMonth=pageMonth.rjust(2, '0')
pageDay=pageDay.rjust(2, '0')
pageHour=pageHour.rjust(2, '0')

#local path for logging
localBase=sys.path[0]+'/'

#source data url
urlPrefix='https://dumps.wikimedia.org/other/pagecounts-raw/'
urlDate = pageYear+'/'+pageYear+'-'+pageMonth
urlFile = '/pagecounts-'+pageYear+pageMonth+pageDay+'-'+pageHour+'0000.gz'
url=urlPrefix+urlDate+urlFile

#raw data path 
localRaw=localBase+'raw/'+urlDate
mkdir_p(localRaw)
#pickle path and file name
localPkl=localBase+'pkl/'+urlDate
mkdir_p(localPkl)
pklFileName=urlFile.split('.')[0]+'.pkl'
#output path and file name
localRes=localBase+'res/'+urlDate
mkdir_p(localRes)
resFileName=urlFile.split('.')[0]+'.csv'

#Part 1. download file if doesn't exist
rawPathFile=localRaw+urlFile
if os.path.exists(rawPathFile):
	print 'File exists localy! Skip downlowd...' 
else:
	print 'File does NOT exist localy! downloading...'
	r = requests.get(url, allow_redirects=True)
	#get response code
	open(rawPathFile, 'wb').write(r.content)

#Part 2. read or create pickle
pklPathFile=localPkl+pklFileName
if os.path.exists(pklPathFile):
	print 'File localy pickled! Reading pickle', pklPathFile
	df = pd.read_pickle(pklPathFile)
else:
	print 'Pickle does NOT exist! Creating...'
	print 'Loading raw data into DataFrame...'
	df = pd.read_csv(localRaw+urlFile, compression='gzip', header=None, sep=' ')
	print 'Pickling DataFrame', pklPathFile
	df.to_pickle(pklPathFile)

# Part 3. aggregate result if output file does not exist
resPathFile=localRes+resFileName
if os.path.exists(resPathFile):
	print 'Output file exists! Reading ', resPathFile
	dfRes=pd.read_csv(resPathFile)
else:
	#dataframe column names and remove non ascii
	df.columns=['language', 'page_name', 'non_unique_views', 'bytes_transferred']
	df.page_name.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)

	#remove recs having page names prefixed e.g "Special:" and languages with dot"."
	recCntbfrDel=len(df)
	df=df[df['page_name'].str.contains(':')==False]
	df=df[df['language'].str.contains('\.')==False]
	print str(recCntbfrDel - len(df)), 'recodes droped due to exclusions'

	#df=df.reset_index(drop=True)
	#aggregate count of visits per language and page name and list top 10 for each category
	dfAgg=df.groupby(['language', 'page_name'])['non_unique_views'].agg({'count':sum})
	dfGrp=dfAgg['count'].groupby(level=0, group_keys=False)
	dfRes=dfGrp.nlargest(10)
	dfRes.to_csv(resPathFile, sep=',')

#print output
print dfRes




