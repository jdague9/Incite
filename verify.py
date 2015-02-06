# -*- coding: utf-8 -*-
"""
Created on Fri Feb 06 10:44:01 2015

@author: JDD46
"""
import csv
import json
import urllib
import mmurl
import sqlite3 as sql

MM_URL = 'https://api.mutualmind.net/v1/campaigns/'

def is_InciteDB(db_fname):
    '''Checks that the input file is a properly configured InciteDB file.
    Raises and IOError if not.'''
    try:
        dbfile = open(db_fname, 'rU')
    except:
        raise IOError('File %s does not exist. Try a different file name.' % (db_fname,))
    else:
        dbfile.close()
        conn = sql.connect(db_fname)
        with conn:
            cur = conn.cursor()
            tables = set(['Campaigns', 'Posts', 'Words', 'PostIndex', 'WordIndex',
                      'Keywords', 'Matches'])
            cur.execute('SELECT name FROM sqlite_master WHERE type = "table"')
            tables_in_db = {name[0] for name in cur.fetchall()}
            if tables_in_db != tables:
                raise IOError('''File %s Does not contain the correct tables! File may
                    not be an InciteDB file or the data may be corrupted.'''
                    % (db_fname,))
                        
def is_MMdatafile(data_fname):
    '''Checks that the input file is a properly formatted Mutual Mind export file.
    Raises and IOError if not.'''
    try:
        datafile = open(data_fname, 'rU')
    except IOError: 
        raise IOError('Input data file not found. Try again')
    else:
        headers = ['publish_timestamp', 'publisher', 'user_name',
                   'location', 'user_score', 'followers_count',
                   'friends_count', 'content_title', 'destination_url',
                   'sentiment', 'matched_categories', 'matched_terms',
                   'applied_labels', 'content_generator', 'user_id',
                   'content_id']
        test_reader = csv.reader(datafile, dialect='excel')
        row = test_reader.next()
        for ind, header in enumerate(headers):
            if row[ind] != header:
                raise IOError('''File %s is not a properly formatted Mutual 
                Mind Export CSV. Export the file again.''' % (data_fname,))
        datafile.close()
        
def is_MMcampaign(extractor, mm_campaign_name):
    '''Checks that the specified Mutual Mind campaign is active on Mutual Mind.
    Raises and IOError if not.'''
    acct_url = MM_URL + 'campaign/'
    get_id_url = mmurl.augment(acct_url, {})
    connection = urllib.urlopen(get_id_url)
    data = connection.read()
    try:
        js = json.loads(data)
    except ValueError:
        raise IOError('No JSON object found. No active campaigns or bad URL')
    # Now see if the data for particular campaign we are looking for is available.
    found = False
    for camp in js['objects']:
        if camp['name'] == mm_campaign_name:
            extractor.mm_campaign_id = camp['id']
            found = True
    if not found:
        raise IOError('Campaign not found. Has the campaign been paused?')