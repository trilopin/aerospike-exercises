import aerospike
from config import config, NAMESPACE
from csv import DictReader


def fix_type(value):
    """
    input: float with 1 decimal representing cm and str
    output: integer representing mm and str
    """
    try:
        return int(round(float(value)*100))
    except ValueError:
        pass

    return value

client = aerospike.client(config).connect()

with open('./data/iris.csv') as csvfile:
    counter = 0
    reader = DictReader(csvfile)
    for row in reader:
        key = (NAMESPACE, 'iris', counter)
        row = {k: fix_type(v) for k, v in row.items()}
        client.put(key, row)
        counter += 1
