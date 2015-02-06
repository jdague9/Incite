# -*- coding: utf-8 -*-
"""
Created on Mon Feb 02 11:03:14 2015

@author: JDD46
"""
import csv
import string
import nltk
import mmurl
import urllib
import json
import verify
import sqlite3 as sql
from nltk.corpus import stopwords

STOP_WORDS = set(stopwords.words('english') + stopwords.words('spanish') + ['rt', 
              'n\'t', 'lol', 'http', 'www', 'com', 'ca', 'amp'])

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
            print 'Error in db_setup: Invalid input. db_fname must be type \'str\''
        else:
            try:
                dbfile = open(db_fname, 'rU')
            except IOError:
                self.db_fname = db_fname
                conn = sql.connect(self.db_fname)
                cur = conn.cursor()
                cur.execute('''
                            CREATE TABLE IF NOT EXISTS Campaigns(
                                id          INTEGER PRIMARY KEY, 
                                name        TEXT UNIQUE
                            )''')
                cur.execute('''
                            CREATE TABLE IF NOT EXISTS Posts(
                                id          INTEGER PRIMARY KEY, 
                                camp_id     INTEGER,
                                text        TEXT,
                                retweets    INTEGER, 
                                is_junk     BOOLEAN, 
                                score       FLOAT,
                                timestamp   TEXT, 
                                source      TEXT, 
                                user_name   TEXT, 
                                user_score  INTEGER, 
                                followers   INTEGER, 
                            FOREIGN KEY(camp_id) REFERENCES Campaign(id)
                            )''')
                cur.execute('''
                            CREATE TABLE IF NOT EXISTS Words(
                                id          INTEGER PRIMARY KEY,
                                name        TEXT,
                                pos         TEXT,
                            UNIQUE(name, pos)
                            )''')
                cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS WordInd ON Words (name)')
                cur.execute('''
                            CREATE TABLE IF NOT EXISTS PostIndex(
                                post_id     INTEGER,
                                word_id     INTEGER
                            )''')
                cur.execute('''
                            CREATE TABLE IF NOT EXISTS WordIndex(
                                word_id     INTEGER,
                                post_id     INTEGER,
                            UNIQUE(word_id, post_id)
                            )''')
                cur.execute('''
                            CREATE TABLE IF NOT EXISTS Keywords(
                                id          INTEGER PRIMARY KEY,
                                camp_id     INTEGER,
                                name        TEXT,
                                rule        TEXT, 
                            UNIQUE(camp_id, name),
                            FOREIGN KEY(camp_id) REFERENCES Campaigns(id)
                            )''')
                cur.execute('''
                            CREATE TABLE IF NOT EXISTS Matches(
                                kw_id       INTEGER,
                                post_id     INTEGER, 
                            UNIQUE(kw_id, post_id)
                            )''')
                cur.close()
                print 'Success! Database file created and configured:', self.db_fname
            else:
                dbfile.close()
                print '''Error in db_setup: File already exists. Choose new
                    file name or call db_connect method.'''
                
    def db_connect(self, db_fname):
        '''Stores the file name of an existing DB file into self.db_fname.
        Raises exceptions if the file doesn't exist or if the requested DB does
        not have the proper tables.'''
        if type(db_fname) is not str:
            print 'Error in db_connect: Invalid input. db_fname must be type \'str\''
        else:
            try:
                verify.is_InciteDB(db_fname)
            except IOError, err:
                print 'Error in db_connect:', err
            else:
                self.db_fname = db_fname
                print 'Success! Target database set:', self.db_fname
        
    def set_data_filename(self, data_fname):
        if type(data_fname) is not str:
            print 'Error in set_data_filename: Invalid input. data_fname must be type \'str\''
        else:
            try:
                verify.is_MMdatafile(data_fname)
            except IOError, err:
                print 'Error in set_data_filename:', err
            else:
                self.data_fname = data_fname
                print 'Success! Data filename set:', self.data_fname
        
    def fetch_keyword_data(self, mm_campaign_name):
        '''Check to see if the Mutual Mind campaign want to work with is
        available on the Mutual Mind API. Checks if there are any active
        campaigns and then whether the requested campaign is active. If 
        everything is in order, pulls down the keyword information for the
        specified campaign.'''
        if self.db_fname is None:
            print '''Error in fetch_keyword_data: Target DB must be set up 
                before fetching keyword data! Call db_setup method or db_connect method.'''
        else:
            try:
                verify.is_MMcampaign(self, mm_campaign_name)
            except IOError, err:
                print 'Error in fetch_keyword_data:', err
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
                    # First input the data about the MM campaign.
                    try:
                        cur.execute('INSERT INTO Campaigns (id, name) VALUES (?, ?)',
                                    (self.mm_campaign_id, self.mm_campaign_name))
                    except sql.IntegrityError:
                        print 'Warning! Data for Mutual Mind Campaign', self.mm_campaign_name,\
                            'is already contained DB file', self.db_fname, '''. Has
                            this data already been ingested to the DB file?'''
                    kw_count = 0
                    for kw in js['objects']:
                        try:
                            cur.execute('''INSERT INTO Keywords (camp_id, name, rule)
                                        VALUES (?, ?, ?)''', (self.mm_campaign_id, kw['name'],
                                        kw['keyword']))
                            kw_count += 1
                        except sql.IntegrityError:
                            print 'Warning! Keyword', kw, 'for campaign',\
                            self.mm_campaign_name,'is already contained in DB file',\
                            self.db_fname
                    conn.commit()
                    cur.execute('SELECT COUNT(*) FROM Keywords')
                    kw_total = cur.fetchone()[0]
                print 'Success! Keyword data fetched for Mutual Mind Campaign:', self.mm_campaign_name
                print 'Keywords stored:', kw_count
                print 'Total keywords in DB:', kw_total
        
    def extract(self):
        '''Extracts data using CSV Reader object created during initialization.
        '''
        if (self.db_fname is None) or (self.mm_campaign_id is None) or (self.data_fname is None):
            print 'Error in extract: all InciteExtractor parameters must be set!'
            print self
        else:
            conn = sql.connect(self.db_fname)
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
                            if row[1] != 'twitter':
                                continue
                            # Remove non-Unicode characters.
                            text = (filter(lambda x: x in (string.printable), row[7]))
                            # Insert relevant post data into Posts table. Each row 
                            # contains data for a different post.
                            try:
                                cur.execute('''INSERT INTO Posts (camp_id, text, retweets,
                                            is_junk, timestamp, source, user_name, user_score, 
                                            followers) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)''', 
                                            (self.mm_campaign_id, text, False, row[0], 
                                             row[1], row[2], row[4], row[5]))
                            except sql.ProgrammingError:
                                print 'Error inserting post:', text
                                continue
    #                        conn.commit()
                            postcount += 1
                            if postcount % 100 == 0:
                                print postcount, 'posts ingested.'
    #                        print '\n\nPost:', text
                            # Now loop through the matched keywords (found in CSV) and
                            # make an entry into the Matches table that tracks which posts
                            # were matched with which keywords during listening.
    #                        print 'Matched Keywords:'
                            post_id = cur.lastrowid
                            keywords = row[11].split('|')
                            for kw in keywords:
                                cur.execute('''SELECT id FROM Keywords WHERE name = ?
                                            LIMIT 1''', (kw,))
                                try:
                                    kw_id = cur.fetchone()[0]
                                except:
                                    print '\tWarning! Keyword', kw, 'not found in Keywords Table!'
                                else:
                                    cur.execute('''INSERT OR IGNORE INTO Matches (kw_id,
                                                post_id) VALUES (?, ?)''', (kw_id, post_id))
    #                                conn.commit()
    #                                print '\t', kw
                            # Now clean up the text and loop through each word to store
                            # relevant word data in the DB.
                                           
                            small = text.lower()
                            tokens = nltk.word_tokenize(small)
                            skip_next = False
                            for token in tokens:
                                if skip_next == True:
                                    skip_next = False
                                    continue
                                if token is ('@' or '#'):
                                    skip_next = True
                                    continue
                                if token[0] not in string.letters:
                                    continue
                                if (token in STOP_WORDS):
                                    wordcount += 1
                                    continue
                                word = lemma.lemmatize(token)
    #                            print '', word,
                                cur.execute('SELECT id from Words WHERE name = ? LIMIT 1',
                                            (word,))
                                try:
                                    word_id = cur.fetchone()[0]
                                except:
                                    cur.execute('''INSERT OR IGNORE INTO Words (name, pos)
                                                VALUES (?, ?)''', (word, None))
    #                                conn.commit()
                                    if cur.rowcount != 1:
                                        print 'Error inserting word:', word
                                        continue
                                    word_id = cur.lastrowid
                                cur.execute('''INSERT INTO PostIndex (post_id, 
                                            word_id) VALUES (?, ?)''', (post_id, word_id))
    #                            conn.commit()
                                cur.execute('''INSERT OR IGNORE INTO WordIndex (word_id,
                                            post_id) VALUES (?, ?)''', (word_id, post_id))
    #                            conn.commit()
                                wordcount += 1
                            conn.commit()
                    else:
                        for row in reader:
                            if row[1] != 'twitter':
                                continue
                            # Remove non-Unicode characters.
                            text = (filter(lambda x: x in (string.printable), row[7]))
                            # Insert relevant post data into Posts table. Each row 
                            # contains data for a different post.
                            try:
                                cur.execute('''INSERT INTO Posts (camp_id, text, retweets,
                                            is_junk, timestamp, source, user_name, user_score, 
                                            followers) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)''', 
                                            (self.mm_campaign_id, text, False, row[0], 
                                             row[1], row[2], row[4], row[5]))
                            except sql.ProgrammingError:
                                print 'Error inserting post:', text
                                continue
    #                        conn.commit()
                            postcount += 1
                            if postcount % 100 == 0:
                                print postcount, 'posts ingested.'
    #                        print '\n\nPost:', text
                            # Now loop through the matched keywords (found in CSV) and
                            # make an entry into the Matches table that tracks which posts
                            # were matched with which keywords during listening.
    #                        print 'Matched Keywords:'
                            post_id = cur.lastrowid
                            keywords = row[11].split('|')
                            for kw in keywords:
                                cur.execute('''SELECT id FROM Keywords WHERE name = ?
                                            LIMIT 1''', (kw,))
                                try:
                                    kw_id = cur.fetchone()[0]
                                except:
                                    print '\tWarning! Keyword', kw, 'not found in Keywords Table!'
                                else:
                                    cur.execute('''INSERT OR IGNORE INTO Matches (kw_id,
                                                post_id) VALUES (?, ?)''', (kw_id, post_id))
    #                                conn.commit()
    #                                print '\t', kw
                                                
                            # Now clean up the text and loop through each word to store
                            # relevant word data in the DB.          
                            small = text.lower()
                            tokens = nltk.pos_tag(nltk.word_tokenize(small))
                            skip_next = False
                            for token, pos in tokens:
                                if skip_next == True:
                                    skip_next = False
                                    continue
                                if token is ('@' or '#'):
                                    skip_next = True
                                    continue
                                if token[0] not in string.letters:
                                    continue
                                if (token in STOP_WORDS):
                                    wordcount += 1
                                    continue
                                if pos[0] == 'N':
                                    word = lemma.lemmatize(token, 'n')
                                if pos[0] == 'V':
                                    word = lemma.lemmatize(token, 'v')
                                elif pos[0] == 'J':
                                    word = lemma.lemmatize(token, 'a')
                                elif pos[0] == 'R':
                                    word = lemma.lemmatize(token, 'r')
                                else:
                                    word = lemma.lemmatize(token)
                                
    #                            print '', word,
                                cur.execute('''SELECT id from Words WHERE name = ? 
                                            LIMIT 1''', (word,))
                                try:
                                    word_id = cur.fetchone()[0]
                                except:
                                    cur.execute('''INSERT OR IGNORE INTO Words 
                                                (name, pos) VALUES (?, ?)''',\
                                                (word, None))
    #                                conn.commit()
                                    if cur.rowcount != 1:
                                        print 'Error inserting word:', word
                                        continue
                                    word_id = cur.lastrowid
                                cur.execute('''INSERT INTO PostIndex (post_id, 
                                            word_id) VALUES (?, ?)''', (post_id, word_id))
    #                            conn.commit()
                                cur.execute('''INSERT OR IGNORE INTO WordIndex (word_id,
                                            post_id) VALUES (?, ?)''', (word_id, post_id))
    #                            conn.commit()
                                wordcount += 1
                            conn.commit()
            print '\nIngested: Posts:', postcount, 'Salient Words:', wordcount                