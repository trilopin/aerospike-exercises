import aerospike
import aerospike.predicates as p
import sys
import os
sys.path.append(os.path.abspath('../'))
from config import config, NAMESPACE

client = aerospike.client(config).connect()
client.udf_put('./filter_and_group_count.lua')
client.index_integer_create(NAMESPACE, 'iris', 'petal_width', 'idx_petal_width')


def filter_and_group_count(filter, groupby):
    res = []

    def add_record(record):
        res.append(record)

    query = client.query(NAMESPACE, 'iris')
    query.where(p.between(filter[0], filter[1], filter[2]))
    query.apply('filter_and_group_count', 'count', [groupby])
    query.foreach(add_record)
    return res

# COUNT ALL ITEMS GROUP BY SPECIES
res = filter_and_group_count(('petal_width', 0, 2000), 'species')
print('count all items: {0}'.format(res[0]))
assert len(res) == 1
assert res[0] == {'virginica': 50, 'versicolor': 50, 'setosa': 50}

# COUNT ALL ITEMS WITH PETAL WIDTH BETWEEN 0 AND 200 MM  GROUP BY SPECIES
res = filter_and_group_count(('petal_width', 0, 200), 'species')
print('count all items with petal_width between 0 and 200 mm: {0}'.format(res[0]))
assert len(res) == 1
assert res[0] == {'virginica': 27, 'versicolor': 50, 'setosa': 50}

# COUNT ALL  ITEMS WITH PETAL WIDTH BETWEEN 50 AND 90 MM  GROUP BY SPECIES
res = filter_and_group_count(('petal_width', 50, 90), 'species')
print('count all items with petal_width between 50 and 90 mm: {0}'.format(res[0]))
assert len(res) == 1
assert res[0] == {'setosa': 2}
