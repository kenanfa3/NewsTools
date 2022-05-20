#!/usr/bin/env python

from elasticsearch import helpers
from elasticsearch import Elasticsearch, TransportError
import argparse
import gzip
import json
import re
import sys
import traceback
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Index WashingtonPost docs to ElasticSearch')
parser.add_argument('bundle', help='WaPo bundle to index')
parser.add_argument('--host', default='localhost', help='Host for ElasticSearch endpoint')
parser.add_argument('--port', default='9200', help='Port for ElasticSearch endpoint')
parser.add_argument('--index_name', default='wapo', help='index name')
parser.add_argument('--create', action='store_true')

args = parser.parse_args()
       
es = Elasticsearch(hosts=[{"host": args.host, "port": args.port}],
                   retry_on_timeout=True, max_retries=10)
settings = {
    'settings': {
        'index': {
            # Optimize for loading; this gets reset when we're done.
            'refresh_interval': '-1',
            'number_of_shards': '5',
            'number_of_replicas': '0',
        },
        # Set up a custom unstemmed analyzer.
        'analysis': {
            'analyzer': {
                'english_exact': {
                    'tokenizer': 'standard',
                    'filter': [
                        'lowercase'
                    ]
                }
            }
        }
    },
    'mappings': {
        'properties': {
            'text': {
                # text is stemmed; text.exact is not.
                'type': 'text',
                'analyzer': 'english',
                'fields': {
                    'exact': {
                        'type': 'text',
                        'analyzer': 'english_exact'
                    }
                }
            }
        }
    }
}

if args.create or not es.indices.exists(index=args.index_name):
    try:
        es.indices.create(index=args.index_name, body=settings)
    except TransportError as e:
        print(e.info)
        sys.exit(-1)

def get_first_content_by_type(jsarr, t):
    for block in jsarr:
        if block is not None and block['type'] == t:
            return block['content']
    return None

def get_all_content_by_type(jsarr, t, field='content'):
    strings = [c[field] for c in jsarr if c is not None and c['type'] == t and field in c and c[field] is not None]
    if strings:
        return ' '.join(strings)
    else:
        return None


def unique_heads(entry):
    items = set()
    if type(entry) is list:
        for x in entry:
            items.add(x[0])
        return list(items)
    else:
        return entry


def doc_generator(f, num_docs):
    for line in tqdm(f, total=num_docs):
        js = json.loads(line)
        try:
            text = get_all_content_by_type(js['contents'], 'sanitized_html')
            links = []
            if text:
                links = re.findall('href="([^"]*)"', text)
                text = re.sub('<.*?>', ' ', text)

            data_dict = {
                "_index": args.index_name,
                "_type": '_doc',
                "_id": js['id'],
            }
            source_block = {
                "title": get_all_content_by_type(js['contents'], 'title'),
                "date": get_first_content_by_type(js['contents'], 'date'),
                "kicker": get_first_content_by_type(js['contents'], 'kicker'),
                "author": js['author'],
                "text": text or '',
                "captions": get_all_content_by_type(js['contents'], 'image', field='fullcaption'),
                "links": links or [],
                "url": js['article_url'],
                "orig": line,
            }

            for key, val in js.items():
                if key == key.upper():
                    source_block[key] = unique_heads(val)

            data_dict['_source'] = source_block

        except Exception:
            # print(json.dumps(js,sort_keys=True, indent=4))
            traceback.print_exc(file=sys.stdout)
            quit()

        yield data_dict

# print("Counting...")
# with open(args.bundle, 'r') as f:
#     lines = 0
#     for line in f:
#         lines += 1
lines = 728626
print("Indexing...")
with open(args.bundle, 'r') as f:
    helpers.bulk(es, doc_generator(f, lines), request_timeout=30)

es.indices.put_settings(index=args.index_name,
                        body={'index': { 'refresh_interval': '1s',
                                         'number_of_replicas': '1',
                        }})
