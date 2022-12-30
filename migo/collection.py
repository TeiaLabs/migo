from pymongo.collection import Collection as MongoCollection
from pymilvus import Collection as MilvusCollection


class Collection:
    def __init__(
        self,
        mongo_collection: MongoCollection,
        milvus_collection: MilvusCollection | None,
    ) -> None:
        self.__mongo_collection = mongo_collection
        self.__milvus_collection = milvus_collection
