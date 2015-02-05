# -*- coding: utf-8 -*-
"""
Created on Mon Feb 02 11:03:14 2015

@author: JDD46
"""
import csv
import string
import math
import numpy as np
import scipy.spatial.distance as dist
import nltk
import re
import mmurl
import urllib
import json
import sqlite3 as sql
from nltk.corpus import stopwords

STOP_WORDS = stopwords.words('english') + stopwords.words('spanish') + ['rt', 
              'n\'t', 'lol']

class InciteExtractor(object):
    '''Initiates and contains CSV Reader object(s) and DB info.'''
    
    def __init__(self, pos_tag=False):
        self.data_fname = None        
        self.mm_campaign_name = None
        self.pos_tag = pos_tag
        self.db_fname = None
        self.mm_campaign_id = None
        self.mm_url = 'https://api.mutualmind.net/v1/campaigns/'
        
        # Check to make sure all the input args are good. If we have anything
        # that doesn't work, then raise and exception so the object is never
        # actually created.
        if type(self.pos_tag) is not bool:
            raise TypeError('Invalid arg for pos_tag! Must be type \'bool\'.')
        
        print 'Success! InciteExtractor created.'
            
    def __repr__(self):
        return '<InciteExtractor>\nCampaign name: ' + str(self.mm_campaign_name) + \
                '\nCampaign ID: ' + str(self.mm_campaign_id) + '\nDB filename: '\
                + str(self.db_fname) + '\nData Filename: ' + str(self.data_fname) + \
                '\npos_tag: ' + str(self.pos_tag)
                
    def db_setup(self, db_fname):
        '''Creates new DB file for extracted data storage. Stores DB file name
        as self.db_fname. Terminates if file already exists.'''
        if type(db_fname) is not str:
            raise TypeError('Invalid input to db_setup. db_fname must be type \'str\'')
        try:
            dbfile = open(db_fname, 'rU')
        except:
            self.db_fname = db_fname
            conn = sql.connect(self.db_fname)
            cur = conn.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS Campaigns (id INTEGER 
                        PRIMARY KEY, name TEXT UNIQUE)''')
            cur.execute('''CREATE TABLE IF NOT EXISTS Posts (id INTEGER PRIMARY
                        KEY, text TEXT, retweets INTEGER, is_junk BOOLEAN, 
                        score FLOAT, timestamp TEXT, source TEXT, user_name 
                        TEXT, user_score INTEGER, followers INTEGER)''')
            cur.execute('''CREATE TABLE IF NOT EXISTS Words (id INTEGER PRIMARY
                        KEY, name TEXT UNIQUE, pos TEXT)''')
            cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS WordInd ON Words (name)')
            cur.execute('''CREATE TABLE IF NOT EXISTS PostIndex (camp_id 
                        INTEGER, post_id INTEGER, word_id INTEGER)''')
            cur.execute('''CREATE TABLE IF NOT EXISTS WordIndex (word_id 
                        INTEGER, post_id INTEGER, UNIQUE(word_id, post_id))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS Keywords (id INTEGER
                        PRIMARY KEY, camp_id INTEGER, name TEXT, rule 
                        TEXT, UNIQUE(camp_id, name))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS KeywordIndex (key_id,
                        camp_id, UNIQUE(key_id, camp_id))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS Matches (kw_id INTEGER,
                        post_id INTEGER, UNIQUE(kw_id, post_id))''')
            cur.close()
            print 'Success! Database file created and configured:', self.db_fname
        else:
            dbfile.close()
            raise IOError('''Error in db_setup! File already exists. Choose new
                file name or call db_connect method.''')
                
    def db_connect(self, db_fname):
        '''Stores the file name of an existing DB file into self.db_fname.
        Raises exceptions if the file doesn't exist or if the requested DB does
        not have the proper tables.'''
        if type(db_fname) is not str:
            raise TypeError('Invalid input in db_connect. db_fname must be type \'str\'')
        try:
            dbfile = open(db_fname, 'rU')
        except:
            raise IOError('''Error in db_connect! File does not exist. Try a 
                different file name or call db_setup method to create and 
                configure a new DB file.''')
        else:
            dbfile.close()
            conn = sql.connect(db_fname)
            cur = conn.cursor()
            tables = ['Campaigns', 'Posts', 'Words', 'PostIndex', 'WordIndex',
                      'Keywords', 'KeywordIndex', 'Matches']
            for table in tables:
                cur.execute('PRAGMA table_info(?)', (table,))
                try:
                    cur.fetchone()[0]
                except:
                    raise NameError('''Error connecting to DB: DB is missing 
                        one or more tables! Data is corrupted. Create new DB.''' )
        self.db_fname = db_fname
        cur.close()
        print 'Success! Target database set:', self.db_fname
        
    def set_data_filename(self, data_fname):
        if type(data_fname) is not str:
            raise TypeError('Invalid arg for set_data_filename. Must be type \'str\'')
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
            for ind, header in headers:
                if row[ind] != header:
                    raise IOError('''Error setting data filename! File %s is 
                        not a properly formatted Mutual Mind Export CSV. 
                        Export the file again.''' % (data_fname,))
            datafile.close()
            self.data_fname = data_fname
            print 'Success! Data filename set:', self.data_fname
        
    def fetch_keyword_data(self, mm_campaign_name):
        '''Check to see if the Mutual Mind campaign want to work with is
        available on the Mutual Mind API. Checks if there are any active
        campaigns and then whether the requested campaign is active. If 
        everything is in order, pulls down the keyword information for the
        specified campaign.'''
        if self.db_fname is None:
            raise IOError('''Target DB must be set up before fetching keyword data!
                Call db_setup method or db_connect method.''')
        acct_url = self.mm_url + 'campaign/'
        get_id_url = mmurl.augment(acct_url, {})
        connection = urllib.urlopen(get_id_url)
        data = connection.read()
        try:
            js = json.loads(data)
        except ValueError:
            raise ValueError('''Error in fetch_keyword_data: No JSON object 
                found. No active campaigns or bad URL''')
        # Now see if the data for campaign we are looking for is available.
        for camp in js['objects']:
            if camp['name'] == mm_campaign_name:
                self.mm_campaign_id = camp['id']
        if self.mm_campaign_id == None:
            raise IOError('''Error in fetch_keyword_data: Campaign not found.
                Bad arg for mm_campaign_name. Has the campaign been paused?''')
        else:
            self.mm_campaign_name = mm_campaign_name
            
        # Here, the Keywords table is populated by reading data about the 
        # Mutual Mind keyword rules from the Mutual Mind API.
        # !!! MAKE SURE THAT THE MUTUAL MIND CAMPAIGN IS NOT PAUSED !!!
        
        # Fist connect to the part of the Mutual Mind API that holds keyword info.
        camp_url = self.mm_url + 'keyword/'
        get_kw_url = mmurl.augment(camp_url, 
                                   {'campaign_id':str(self.mm_campaign_id)})
        connection = urllib.urlopen(get_kw_url)
        # Next, pull down the JSON representation of the data.
        data = connection.read()
        js = json.loads(data)
        # Now, put the data into our DB file.
        conn = sql.connect(self.db_fname)
        with conn:
            cur = conn.cursor()
            kw_count = 0
            for kw in js['objects']:
                cur.execute('''INSERT OR IGNORE INTO Keywords (id, name, rule)
                            VALUES (?, ?, ?)''', (self.mm_campaign_id, kw['name'],
                            kw['keyword']))
                kw_count += 1
            conn.commit()
            cur.execute('SELECT COUNT(*) FROM Keywords')
            kw_total = cur.fetchone()[0]
        print 'Success! Keyword data fetched for Mutual Mind Campaign:', self.mm_campaign_name
        print 'Keywords fetched:', kw_count
        print 'Total keywords in DB:', kw_total
        
    def extract(self):
        '''Extracts data using CSV Reader object created during initialization.
        '''
        if self.db_fname is None:
            raise IOError('''Target database must be specified! Call db_setup or
                db_connect method.''')       
        if self.mm_campaign_id is None:
            raise IOError('''Mutual Mind Campaign must be specified! Call
                fetch_keyword_data method.''')
        if self.data_fname is None:
            raise IOError('''Input data file must be specified! Call
                set_data_filename method.''')
        conn = sql.connect()
        with conn:
            cur = conn.cursor()
            # Now it's time to read through the CSV and store the data in the DB
            # file. Skip the first line of the data... It's just headers.
            with open(self.data_fname, 'rU') as csv_file:
                reader = csv.reader(csv_file, dialect='excel')
                reader.next()
                postcount = 0
                wordcount = 0
                lemma = nltk.stem.WordNetLemmatizer()
                if self.pos_tag == False:
                    for row in reader:
                        # Remove non-Unicode characters.
                        text = (filter(lambda x: x in (string.printable), row[7]))
                        # Insert relevant post data into Posts table. Each row 
                        # contains data for a different post.
                        cur.execute('''INSERT INTO Posts (text, retweets,
                                    is_junk, timestamp, source, user_name, user_score, 
                                    followers) VALUES (?, 0, ?, ?, ?, ?, ?, ?)''', 
                                    (text, False, row[0], row[1], row[2], row[4],
                                     row[5]))
                        conn.commit()
                        postcount += 1
    #                    if postcount % 100 == 0:
    #                        print postcount, 'posts ingested.'
                        print 'Post:', text
                        # Now loop through the matched keywords (found in CSV) and
                        # make an entry into the Matches table that tracks which posts
                        # were matched with which keywords during listening.
                        print '\t Matched Keywords:'
                        post_id = cur.lastrowid
                        keywords = row[11].split('|')
                        for kw in keywords:
                            cur.execute('''SELECT id FROM Keywords WHERE name = ?
                                        LIMIT 1''', (kw,))
                            try:
                                kw_id = cur.fetchone()[0]
                            except:
                                print '\t\tWarning! Keyword', kw, 'not found in Keywords Table!'
                            else:
                                cur.execute('''INSERT OR IGNORE INTO Matches (kw_id,
                                            post_id) VALUES (?, ?)''', (kw_id, post_id))
                                conn.commit()
                                print '\t\t', kw
                        # Now clean up the text and loop through each word to store
                        # relevant word data in the DB.
                                
                        #### TO-DO: Figure out the best way to do tokenizing, stemming.
                        print 'Words:'        
                        small = text.lower()
                        tokens = nltk.word_tokenize(small)
                        for token in tokens:
                            if token[0] not in string.letters:
                                continue
                            if (token in STOP_WORDS) or re.search('^http', token):
                                wordcount += 1
                                continue
                            word = lemma.lemmatize(token)
                            print '', word,
                            ### TO-DO: Error here. sqlite3.ProgrammingError: You must 
                            ### not use 8-bit bytestrings. I suspect a word coming in
                            ### was not unicode. Check on it
                            cur.execute('SELECT id from Words WHERE name = ? LIMIT 1',
                                        (word,))
                            try:
                                word_id = cur.fetchone()[0]
                            except:
                                cur.execute('''INSERT OR IGNORE INTO Words (name, pos)
                                            VALUES (?, ?)''', (word, None))
                                conn.commit()
                                if cur.rowcount != 1:
                                    print 'Error inserting word:', word
                                    continue
                                word_id = cur.lastrowid
                            cur.execute('''INSERT OR IGNORE INTO PostIndex (post_id, 
                                        word_id) VALUES (?, ?)''', (post_id, word_id))
                            conn.commit()
                            cur.execute('''INSERT OR IGNORE INTO WordIndex (word_id,
                                        post_id) VALUES (?, ?)''', (word_id, post_id))
                            conn.commit()
                            wordcount += 1
        print 'Ingested: Posts:', postcount, 'Salient Words:', wordcount                