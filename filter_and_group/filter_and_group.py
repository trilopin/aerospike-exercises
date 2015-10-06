import aerospike
import aerospike.predicates as p
import sys
import os
sys.path.append(os.path.abspath('../'))
from config import config, NAMESPACE

client = aerospike.client(config).connect()
client.udf_put('./filter_and_group.lua')
client.index_integer_create(NAMESPACE, 'iris', 'petal_width', 'idx_petal_width')


def print_group(group):
    for key in group.keys():
        print('\n{0}'.format(key))
        for item in group[key]:
            print('\t{0}'.format(item))

def filter_and_group(filter, groupby):
    res = []

    def add_record(record):
        res.append(record)

    query = client.query(NAMESPACE, 'iris')
    query.where(p.between(filter[0], filter[1], filter[2]))
    query.apply('filter_and_group', 'group', [groupby])
    query.foreach(add_record)
    return res

# ALL ITEMS GROUP BY SPECIES
res = filter_and_group(('petal_width', 0, 2000), 'species')
assert res[0].keys() == ['virginica', 'versicolor', 'setosa']
assert len(res[0]['virginica']) == 50
assert len(res[0]['versicolor']) == 50
assert len(res[0]['setosa']) == 50
print('group all items...ok')
# print_group(res[0])

# ALL ITEMS WITH PETAL WIDTH BETWEEN 0 AND 200 MM  GROUP BY SPECIES
res = filter_and_group(('petal_width', 0, 200), 'species')
assert res[0].keys() == ['virginica', 'versicolor', 'setosa']
assert len(res[0]['virginica']) == 27
assert len(res[0]['versicolor']) == 50
assert len(res[0]['setosa']) == 50
print('group all items with petal_width between 0 and 200 mm...ok')
# print_group(res[0])

# ALL  ITEMS WITH PETAL WIDTH BETWEEN 50 AND 90 MM  GROUP BY SPECIES
res = filter_and_group(('petal_width', 50, 90), 'species')
assert res[0].keys() == ['setosa']
assert len(res[0]['setosa']) == 2
print('group all items with petal_width between 50 and 90 mm...ok')
# print_group(res[0])