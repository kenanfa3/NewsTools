from elasticsearch import helpers
from elasticsearch import Elasticsearch, TransportError
import argparse
import gzip
import json
import re
import traceback
from tqdm.auto import tqdm
import xmltodict
import urllib
import json
import sys
from datetime import datetime, timezone

# Init Elasticsearch
es = Elasticsearch()
# es.indices.get_alias("*")
# INDEX = 'enwiki2020_index_1'
INDEX = 'wapov2_spacy'
es.indices.refresh(INDEX)

TREC_YEAR = 2019 # 2019, using V2 for both
TREC_DATAFILES_DIR = "/DATA/users/kenanfayoumi/EntityRanking/trec_files"
    
with open(f'{TREC_DATAFILES_DIR}/num_to_docid_mapping_TREC{TREC_YEAR}.json', 'r') as fp:
    num_to_docid_mapping=json.load(fp)
docid_to_num_mapping  = {v: k for k, v in num_to_docid_mapping.items()}


run_file = 'res_files/wapoV4_BASELINE_2021'
!rm $run_file

for article_num in tqdm(num_to_docid_mapping.keys()):
    query_article = es.get(index=INDEX, id=num_to_docid_mapping[article_num])
    
    script_query = {
        "bool":{
            "must": [
               {
                   "range": {"date": {"lt": query_article['_source']['date'] }}
               },
               {
                    "multi_match":{
                        "query":query_article['_source']["text"][:5600],
                        "fields":[
                            "text",
                            "author",
                            "title",
                        ]
                    }
               }
            ],
               "must_not": [{
                   'ids':{"values": [query_article['_id']]} 
                   }
#             {
#               "match": {
#                 "kicker": "Letters to the Editor"
                  
#               }
#             },
#             {
#               "match": {
#                 "kicker": "Opinions"
#               }
#             },
#               {
#               "match": {
#                 "kicker": "The Post's View"
#               }
#             }
            

              ]
        }
    }
        

    res = es.search(index=INDEX, query=script_query,size=100)
    for i,hit in enumerate(res['hits']['hits']):
        doc_id = hit['_id']
        score = hit['_score']
        line ='%s Q0 %s %d %f OzU_wiki' % (article_num,doc_id,100-i,score)
        
        with open(run_file,'a') as file:
          file.write(line)
          file.write('\n')


import subprocess
treceval = "/home/kenanfayoumi/trec_eval/trec_eval"
# qrels = "/DATA/projects/TRECNews/2018/bl/bqrels.exp-gains.txt"
# qrels = "/DATA/projects/TRECNews/2019/bl/newsir19-qrels-background.txt"
# qrels = "/DATA/projects/TRECNews/2020/bl/qrels.background"
qrels = "/DATA/projects/TRECNews/2021/bl/qrels.background"

run_file = 'res_files/wapoV4_BASELINE_2021'


result = subprocess.run([treceval,
                         '-M 100',
                         '-q',
                         '-mall_trec',
                         # '-mndcg.1=2,2=4,3=8,4=16',
                         # '-l16',
                         qrels,
                         run_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

print( 'exit status:', result.returncode )
print( 'stdout:', result.stdout.decode() )
print( 'stderr:', result.stderr.decode() )