# -*- coding: utf-8 -*-
"""
Created on Fri Feb 06 10:20:58 2015

@author: JDD46
"""
import verify
import numpy as np
import scipy.spatial.distance as dist
import sqlite3 as sql

def clean(db_fname):
    '''Removes retweets from data stored in a Incite DB file.'''
    try:
        verify.is_InciteDB(db_fname)
    except IOError, err:
        print 'Error in clean:', err
    else: 
        print 'Fetching data...'
        word_id_to_wordind = {}
        wordind_to_word_id = {}
        post_id_to_postind = {}
        postind_to_post_id = {}
        conn = sql.connect(db_fname)
        with conn:
            cur = conn.cursor()
            cur.execute('''select count(distinct word_id) from WordIndex join 
                        Posts on WordIndex.post_id=Posts.id where 
                        Posts.is_junk=?''', (False,))
            num_words = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM Posts WHERE is_junk=?', (False,))
            num_posts = cur.fetchone()[0]
            svec_matrix = np.zeros((num_posts, num_words), dtype=float)
            cur.execute('''select Posts.id, Words.id from PostIndex join Words
                        on PostIndex.word_id=Words.id join Posts on 
                        PostIndex.post_id=Posts.id where Posts.is_junk=0''')
            coords = cur.fetchall()
            print len(coords), 'records fetched.'
            print 'Building sentence vector matrix...'
            wordind, postind = -1, -1
            for post_id, word_id in coords:
                if post_id not in post_id_to_postind:
                    postind += 1
                    post_id_to_postind[post_id] = postind
                    postind_to_post_id[postind] = post_id
                if word_id not in word_id_to_wordind:
                    wordind += 1
                    word_id_to_wordind[word_id] = wordind
                    wordind_to_word_id[wordind] = word_id
                svec_matrix[post_id_to_postind[post_id], 
                            word_id_to_wordind[word_id]] += 1
            print 'Calculating cosine similarities...'    
            cosines = np.triu(dist.squareform(1 - dist.pdist(svec_matrix, 
                                                              'cosine')))
            print 'Identifying retweets...'
            rt_coords = zip(np.asarray(np.where(cosines >= 0.9)[0], dtype=int).tolist(), 
                            np.asarray(np.where(cosines >= 0.9)[1], dtype=int).tolist())
            to_junk = set()
            for post1, post2 in rt_coords:
                if post1 in to_junk:
                    continue
                to_junk.add(post2)
                retweeted_id = postind_to_post_id[post1]
                cur.execute('update Posts set retweets = retweets + 1 where id = ?',
                            (retweeted_id,))
            conn.commit()
            print 'Flagging retweets...'
            flagged = 0
            for post_ind in to_junk:
                pid = postind_to_post_id[post_ind]
                cur.execute('update Posts set is_junk = ? where id = ?', (True, pid))
                flagged += 1
            conn.commit()
                
        print 'Success! %d posts in DB file %s were marked as retweets.'\
                % (flagged, db_fname)
                
                    
            
                        