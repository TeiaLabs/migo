# migo

Wrapper package around pymongo and pymilvus pushing Milvus capacity with document interaction through MongoDB


# Install

`pip install migo`


# Usage


```python
from migo.client import Client

# No configs
c = Client({}, {})

db = c.get_default_database()

# Mongo will automatically create the collection for you
coll = db.get_collection("test")

print(coll.find_one())
```
