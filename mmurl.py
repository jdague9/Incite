# -*- coding: utf-8 -*-
"""
Created on Mon Feb 02 11:03:14 2015

@author: JDD46
"""
import urllib
import hidden

# url = twurl.augment(TWITTER_URL, {'screen_name':acct, 'count':20})
# Goes into urlopen() 

def augment(url, parameters) :
    secrets = hidden.mm_auth()
    key = secrets['key']
    token = secrets['token']
    auth = 'application_key=' + key + '&auth_token=' + token
    params = ['?format=json', 'limit=50']
    for param in parameters.keys():
        params.append(param + '=' + parameters[param])
    params.append(auth)
    aug_url = url + '&'.join(params)
    return aug_url
