# -*- coding: utf-8 -*-
"""
Created on Tue Feb 03 12:27:21 2015

@author: JDD46
"""
from ingest import *

#test = InciteExtractor()
#test.db_setup('../DB/test.db')
#test.set_data_filename('../MM_Data/test.csv')
#test.fetch_keyword_data('Cranberry Juice')
#test.extract()

cran = InciteExtractor(pos_tag=True)
cran.db_setup('../DB/cranberry_pos.db')
cran.set_data_filename('../MM_Data/cranberry.csv')
cran.fetch_keyword_data('Cranberry Juice')
cran.extract()

# SORTS BY COUNT, RETURNS (WORD, COUNT):
# cur.execute('select Words.name, count(*) from PostIndex join Words on PostIndex.word_id=Words.id group by word_id order by count(*) desc')