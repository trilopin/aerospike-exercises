import aerospike
import aerospike.predicates as p
import sys
import os
sys.path.append(os.path.abspath('../'))
from config import config, NAMESPACE

client = aerospike.client(config).connect()
client.index_integer_create(NAMESPACE, 'iris', 'petal_width', 'idx_petal_width')


def limit(lim, result):
    c = [0]
    def key_add((key, metadata, bins)):
        if c[0] < lim:
            result.append(bins)
            c[0] = c[0] + 1
        else:
            return False
    return key_add


def query_with_limit(filter, lim):
    res = []

    def add_record(record):
        res.append(record)

    query = client.query(NAMESPACE, 'iris')
    query.where(p.between(filter[0], filter[1], filter[2]))
    query.foreach(limit(lim, res))
    return res


res = query_with_limit(('petal_width', 0, 2000), 5)
assert len(res) == 5
for r in res:
    print(r)
