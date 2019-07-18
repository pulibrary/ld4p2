#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
For LD4P2: get quarterly updates for share-vde
From http://bit.ly/SVDE_Record_submission
	After the first whole extraction the library should send three different files: new records, updated records, deleted	records.
First report was run 20181116, reviewed by jeb and rh in 201901, and submitted 20190118
See also LD4P-Cohort institutions and content: https://docs.google.com/spreadsheets/d/18LQQAUi_O87OoTpkUmvh9xFCSGUe0P_VArwvQ6Y1eVk/edit#gid=1394834818
From 20190531
pmg
"""

# TODO: run over a full dump

import argparse
import cx_Oracle
import csv
import ConfigParser
import logging
import pandas as pd
import time
from ordered_set import OrderedSet

today = time.strftime('%Y%m%d')

config = ConfigParser.RawConfigParser()
config.read('./conf/vger.cfg')
user = config.get('database', 'user')
pw = config.get('database', 'pw')
sid = config.get('database', 'sid')
ip = config.get('database', 'ip')

dsn_tns = cx_Oracle.makedsn(ip,1521,sid)
db = cx_Oracle.connect(user,pw,dsn_tns)

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename='./logs/'+today+'.log',level=logging.INFO)

previous_run = './svde/in/svde_bibs_initial_201901.csv'
previous_run_date = '20181106'

def get_update():
	'''
	Repeat the first query to get new list of bibs. First report had 5396449 records. 
	'''

	msg='getting the latest results to original query'
	if verbose:
		print(msg)
	logging.info(msg)
	
	query_string = """SELECT DISTINCT BIB_TEXT.BIB_ID
FROM BIB_MASTER
LEFT JOIN BIB_TEXT ON BIB_MASTER.BIB_ID = BIB_TEXT.BIB_ID
WHERE 
BIB_MASTER.SUPPRESS_IN_OPAC = 'N' AND
(REGEXP_LIKE(princetondb.GETALLBIBTAG(BIB_TEXT.BIB_ID, '035','2'),'a\(OCoLC\)') OR
REGEXP_LIKE(princetondb.GETALLBIBTAG(BIB_TEXT.BIB_ID, '035','2'),'a\([a-z]{0,3}RLIN\)','i'))"""
	c = db.cursor()
	c.execute(query_string)
	with open("./svde/in/svde_out_latest_"+today+".csv","wb+") as outfile:
		csv_writer = csv.writer(outfile)
		for record in c.fetchall():
			csv_writer.writerows([record])
	c.close()


def get_changes():
	'''
	Get bibs of records that have changed since the last run
	'''
	
	msg='getting list of records that have changed'
	if verbose:
		print(msg)
	logging.info(msg)
	
	with open(previous_run,'rb') as bibsin:
		csv_reader = csv.reader(bibsin)
		counter = 0
		for row in csv_reader:
			c = db.cursor()
			bib = row[0]
			query_string = """SELECT DISTINCT BIB_HISTORY.BIB_ID FROM BIB_HISTORY WHERE BIB_ID = %s AND BIB_HISTORY.ACTION_DATE > to_date('%s', 'yyyy/mm/dd')""" % (bib,previous_run_date)

			print query_string

			c.execute(query_string)
			for record in c.fetchall():
				with open("./svde/updates/changed_history"+today+".csv","ab+") as outfile:
					csv_writer = csv.writer(outfile)
					csv_writer.writerows([record])
			c.close()
			counter += 1

			msg='%s: %s' % (counter,bib)
			if verbose:
				print(msg)
			logging.info(msg)


def compare():
	'''
	compare update report against previous
	'''

	msg='compare reports to get new, changed, deletes'
	if verbose:
		print(msg)
	logging.info(msg)
	
	latest_bibs = set()
	og_bibs = set()
	changed_bibs = set()
	
	msg='going to read ...'

	if verbose:
		print(msg)
	logging.info(msg)
	original_bibs = pd.read_csv('./svde/in/svde_bibs_initial_201901.csv',header=None)
	#original_bibs = pd.read_csv('./svde/in/initial_deleteme.csv',header=None)
	msg='read original'

	if verbose:
		print(msg)
	logging.info(msg)
	new_bibs = pd.read_csv('./svde/in/svde_bibs_latest_035a_20190621.csv')
	msg='read new'

	if verbose:
		print(msg)
	logging.info(msg)
	recently_changed_bibs = pd.read_csv('./svde/updates/changed_history_20190614.csv',header=None)
	msg='read changed'
	if verbose:
		print(msg)
	logging.info(msg)

	# put bibs from dataframes into sets
	# get the latest set of oclc rlin bibs
	for index,row in new_bibs.iterrows():
		msg='new',index, row[0]
		if verbose:
			print(msg)
		latest_bibs.add(row[0])
	msg='got latest into a set'
	if verbose:
		print(msg)
	logging.info(msg)

	# get the set of original oclc rlin bibs
	for index,row in original_bibs.iterrows():
		msg='og',index, row[0]
		if verbose:
			print 'og',index, row[0]
		og_bibs.add(row[0])
	print 'got original into a set'

	# get the set of changed records (based on bib history)
	for index,row in recently_changed_bibs.iterrows():
		msg='changed',index, row[0]
		if verbose:
			print(msg)
		changed_bibs.add(row[0])
	msg='got changed into a set'
	logging.info(msg)
	if verbose:
		print(msg)

	#compare the sets ...
	deletes = og_bibs - latest_bibs # original_bibs - latest_bibs (based on same query for unsuppressed OCLC or RLIN bibs)
	changes = changed_bibs - deletes # changed bibs - deletes (e.g. change was that 035 is no longer OCoLC or RLIN)
	additions = latest_bibs - og_bibs # new_bibs - original_bibs

	with open('./svde/updates/SVDE_bib_princeton_'+today+'_del_01.csv','wb+') as dels, open('./svde/updates/'+today+'_new_01.csv','wb+') as new, open('./svde/updates/'+today+'_mod_01.csv','wb+') as changed:
		del_writer = csv.writer(dels)
		new_writer = csv.writer(new)
		changes_writer = csv.writer(changed)
		
		for d in sorted(deletes):
			if verbose:
				print(d)
			del_writer.writerow([d])

		for n in sorted(additions):
			if verbose:
				print(n)
			new_writer.writerow([n])

		for c in sorted(changes):
			if verbose:
				print(c)
			changes_writer.writerow([c])

	msg='''deletes: %s'\nnew: %s\nchanged: %s\ndone for now''' % (len(deletes),len(additions),len(changes))
	logging.info(msg)

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Generate uri reports.')
	parser.add_argument("-v", "--verbose", required=False, default=False, dest="verbose", action="store_true", help="Runtime feedback.")
	parser.add_argument("-l", "--update", required=False, default=False, dest="update", action="store_true", help="Get the updated results to original query.")
	parser.add_argument("-c", "--changes", required=False, default=False, dest="changes", action="store_true", help="Get list of bibs that have been changed since the last run.")
	parser.add_argument("-d", "--diffs", required=False, default=False, dest="diffs", action="store_true", help="Compare lists to find additions, deletes, changes.")
	args = vars(parser.parse_args())
	verbose = args['verbose']
	update = args['update']
	changes = args['changes']
	diffs = args['diffs']
	
	if update:
		get_update()
	if changes:
		get_changes()
	if diffs:
		compare()
