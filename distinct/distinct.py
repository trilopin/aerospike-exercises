import aerospike
import aerospike.predicates as p
import sys
import os
sys.path.append(os.path.abspath('../'))
from config import config, NAMESPACE

client = aerospike.client(config).connect()
client.udf_put('./distinct.lua')
client.index_integer_create(NAMESPACE, 'iris', 'petal_width', 'idx_petal_width')

def get_distinct(bin):
    res = []

    def add_record(record):
        res.append(record.keys())

    query = client.query(NAMESPACE, 'iris')
    query.apply('distinct', 'distinct_bin', [bin])
    query.foreach(add_record)
    res[0].sort()
    return res[0]


res = get_distinct('species')
print(res)
assert res == ['setosa',  'versicolor', 'virginica' ]

res = get_distinct('petal_length')
print(res)