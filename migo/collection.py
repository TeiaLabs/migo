from typing import TypeVar, Callable, Any, Generic
from pymongo.collection import Collection as MongoCollection
from pymilvus import Collection as MilvusCollection


F = TypeVar("F", bound=Callable[..., Any])


class copy_sig(Generic[F]):
    def __init__(self, target: F) -> None:
        ...

    def __call__(self, wrapped: Callable[..., Any]) -> F:
        ...


class Filter:
    mongo_filter: dict | None = None
    milvus_filter: dict[str, list] | None = None


class Document:
    mongo_document: dict
    milvus_array: list | None = None


class BatchDocument:
    mongo_documents: list[dict]
    milvus_arrays: list[list] | None = None


class Collection:
    def __init__(
        self,
        mongo_collection: MongoCollection,
        milvus_collection: MilvusCollection | None,
    ) -> None:
        self.__mongo_collection = mongo_collection
        self.__milvus_collection = milvus_collection

    def get_drivers(self) -> tuple[MongoCollection, MilvusCollection | None]:
        return (self.__mongo_collection, self.__milvus_collection)

    # =========== Unified interface ===========

    def find_one(
        self,
        filter: Filter | None = None,
        sort: list[str, str | int] | None = None,
        search_param: dict | None = None,
        partition_names: list[str] | None = None,
    ):
        if filter and filter.milvus_filter:
            milvus_filter = filter.milvus_filter

            name = next(milvus_filter.keys())
            milvus_filter = {name: [milvus_filter[name]]}

        return self.find_many(filter, sort, search_param, partition_names, limit=1)

    def find_many(
        self,
        filter: Filter | None = None,
        sort: list[str, str | int] | None = None,
        search_param: dict | None = None,
        partition_names: list[str] | None = None,
        limit: int = 0,
    ):
        mongo_filter, milvus_filter = None, None
        if filter is not None:
            mongo_filter = filter.mongo_filter
            milvus_filter = filter.milvus_filter

        milvus_ids = {}
        if milvus_filter is not None:
            # Query milvus by vector similarity

            field_name = next(milvus_filter.keys())
            field_value = milvus_filter[field_name]
            milvus_results = self.__milvus_collection.search(
                data=field_value,
                anns_field=field_name,
                param=search_param,
                partition_names=partition_names,
                limit=limit,
            )
            for result in milvus_results:
                for hit in result:
                    milvus_ids[hit.id] = hit

        if mongo_filter is not None and milvus_ids:
            mongo_filter["milvus_id"] = {"$in": list(milvus_ids.keys())}

        mongo_results = self.__mongo_collection.find(mongo_filter, sort, limit=limit)

        final_results = []
        pending_results = {}
        for result in mongo_results:
            milvus_id = result.get("milvus_id", None)
            if milvus_id in milvus_ids:
                result["milvus_data"] = milvus_ids[milvus_id]
                final_results.append(result)
            elif milvus_id is not None:
                pending_results[milvus_id] = result

        if pending_results:
            # Query milvus by mongo's primary keys if needed

            milvus_results = self.__milvus_collection.query(
                f"pk in {list(pending_results.keys())}"
            )
            for result in milvus_results:
                for hit in result:
                    mongo_result = pending_results[hit.id]
                    mongo_result["milvus_data"] = hit
                    final_results.append(mongo_result)

        return final_results

    def insert_one(
        self,
        data: Document,
        partition_name: str,
    ):
        if data.milvus_array is not None:
            result = self.__milvus_collection.insert(
                [data.milvus_array], partition_name
            )
            data.mongo_document["milvus_id"] = result.primary_keys[0]

        return self.__mongo_collection.insert_one(data.mongo_document)

    def insert_many(self, data: BatchDocument, partition_name):
        if data.milvus_arrays is not None:
            result = self.__milvus_collection.insert(data.milvus_arrays, partition_name)
            for document, milvus_pk in zip(data.mongo_documents, result.primary_keys):
                document["milvus_id"] = milvus_pk

        return self.__mongo_collection.insert_many(data.mongo_documents)

    def replace_one(self):
        pass

    def replace_many(self):
        pass

    def update_one(self):
        pass

    def update_many(self):
        pass

    def delete_one(self):
        pass

    def delete_many(self):
        pass

    def distinct(self, key, filter: dict | None = None):
        mongo_results = self.__mongo_collection.distinct(key, filter)

        milvus_ids = {}
        for result in mongo_results:
            milvus_id = result.get("milvus_id", None)
            if milvus_id is not None:
                milvus_ids[milvus_id] = result

        if milvus_ids:
            # Query milvus by mongo's primary keys if needed

            milvus_results = self.__milvus_collection.query(
                f"pk in {list(milvus_ids.keys())}"
            )

            for result in milvus_results:
                for hit in result:
                    milvus_ids[hit.id]["milvus_data"] = hit

        return list(milvus_ids.values())

    def drop(self):
        self.__mongo_collection.drop()
        self.__milvus_collection.drop()

    def count(self, filter: dict | None = None):
        return self.__mongo_collection.count_documents(filter)

    # =========== Mongo specific ===========

    @copy_sig(MongoCollection.create_index)
    def create_index(self, *args, **kwargs):
        return self.__mongo_collection.create_index(*args, **kwargs)

    @copy_sig(MongoCollection.create_indexes)
    def create_indexes(self, *args, **kwargs):
        return self.__mongo_collection.create_indexes(*args, **kwargs)

    @copy_sig(MongoCollection.drop_index)
    def drop_index(self, *args, **kwargs):
        return self.__mongo_collection.drop_index(*args, **kwargs)

    @copy_sig(MongoCollection.drop_indexes)
    def drop_indexes(self, *args, **kwargs):
        return self.__mongo_collection.drop_indexes(*args, **kwargs)

    @copy_sig(MongoCollection.with_options)
    def with_options(self, *args, **kwargs):
        return self.__mongo_collection.with_options(*args, **kwargs)

    @copy_sig(MongoCollection.watch)
    def watch(self, *args, **kwargs):
        return self.__mongo_collection.watch(*args, **kwargs)

    @copy_sig(MongoCollection.list_indexes)
    def list_indexes(self, *args, **kwargs):
        return self.__mongo_collection.list_indexes(*args, **kwargs)

    @copy_sig(MongoCollection.index_information)
    def index_information(self, *args, **kwargs):
        return self.__mongo_collection.index_information(*args, **kwargs)

    # =========== Milvus Specific ===========

    @copy_sig(MilvusCollection.load)
    def load(self, *args, **kwargs):
        self.__milvus_collection.load(*args, **kwargs)

    @copy_sig(MilvusCollection.release)
    def release(self):
        self.__milvus_collection.release()

    @copy_sig(MilvusCollection.partition)
    def partition(self, *args, **kwargs):
        return self.__milvus_collection.partition(*args, **kwargs)

    @copy_sig(MilvusCollection.create_partition)
    def create_partition(self, *args, **kwargs):
        self.__milvus_collection.create_partition(*args, **kwargs)

    @copy_sig(MilvusCollection.has_partition)
    def has_partition(self, *args, **kwargs):
        return self.__milvus_collection.has_partition(*args, **kwargs)

    @copy_sig(MilvusCollection.drop_partition)
    def drop_partition(self, *args, **kwargs):
        return self.__milvus_collection.drop_partition(*args, **kwargs)
