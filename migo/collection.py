from dataclasses import asdict

from pymilvus import Collection as MilvusCollection
from pymongo.collection import Collection as MongoCollection

from .utils import BatchDocument, Document, DropIndex, Field, Filter, Index, copy_sig


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
        fields: list[Field] | None = None,
        search_param: dict | None = None,
        partition_names: list[str] | None = None,
    ) -> dict:
        if filter and filter.milvus_filter:
            milvus_filter = filter.milvus_filter

            name = next(milvus_filter.keys())
            milvus_filter = {name: [milvus_filter[name]]}

        documents = self.find_many(
            filter, sort, fields, search_param, partition_names, limit=1
        )
        return None if not documents else documents[0]

    def find_many(
        self,
        filter: Filter | None = None,
        sort: list[tuple[str, str | int]] | None = None,
        fields: list[Field] | None = None,
        search_param: dict | None = None,
        partition_names: list[str] | None = None,
        limit: int = 0,
    ) -> list[dict]:
        mongo_filter, milvus_filter = None, None
        if filter is not None:
            mongo_filter = filter.mongo_filter
            milvus_filter = filter.milvus_filter

        milvus_ids = {}
        if milvus_filter is not None:
            # Query milvus by vector similarity
            milvus_fields = [field.milvus_field for field in fields] if fields else None

            field_name = next(milvus_filter.keys())
            field_value = milvus_filter[field_name]
            milvus_results = self.__milvus_collection.search(
                data=field_value,
                output_fields=milvus_fields,
                anns_field=field_name,
                param=search_param,
                partition_names=partition_names,
                limit=limit,
            )
            for result in milvus_results:
                for hit in result:
                    milvus_ids[hit.id] = hit

        if mongo_filter is None:
            mongo_filter = {}

        if milvus_ids:
            mongo_filter["milvus_id"] = {"$in": list(milvus_ids.keys())}

        mongo_fields = {field.mongo_field: True for field in fields} if fields else None
        mongo_results = self.__mongo_collection.find(
            mongo_filter, sort, projection=mongo_fields, limit=limit
        )

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
        partition_name: str | None = None,
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

    def replace_one(
        self,
        data: Document,
        filter: Filter | None = None,
        search_param: dict | None = None,
        partition_name: str | None = None,
        upsert: bool = False,
    ):
        if filter.milvus_filter is None:
            return self.__mongo_collection.replace_one(
                filter=filter.mongo_filter,
                replacement=data.mongo_document,
                upsert=upsert,
            )

        document = self.find_one(
            filter=filter,
            fields=[Field(mongo_field="_id", milvus_field="id")],
            search_param=search_param,
            partition_names=[partition_name],
        )

        if not document:
            return

        mongo_result = self.__mongo_collection.replace_one(
            filter={"_id": document["_id"]},
            replacement=data.mongo_document,
            upsert=upsert,
        )

        if mongo_result.upserted_id:
            milvus_result = self.__milvus_collection.insert(
                [data.milvus_array],
                partition_name=partition_name,
            )
        elif mongo_result.matched_count:
            milvus_result = self.__update_milvus(
                arrays=[data.milvus_array],
                documents=[document],
                partition_name=partition_name,
                recover=True,
            )
        else:
            return

        if milvus_result is None or not milvus_result.insert_count:
            # Add the old document back in case something went wrong with milvus
            document.pop("milvus_data")
            self.__mongo_collection.replace_one({"_id": document["_id"]}, document)
            return

        self.__mongo_collection.update_one(
            filter={"_id": document["_id"]},
            update={"milvus_id": milvus_result[0][0].id},
            upsert=upsert,
        )

    def update_one(
        self,
        data: Document,
        filter: Filter | None = None,
        search_param: dict | None = None,
        partition_name: str | None = None,
        upsert: bool = False,
    ):
        if filter.milvus_filter is None:
            self.__mongo_collection.update_one(
                filter.mongo_filter,
                data.mongo_document,
            )
            return

        document = self.find_one(
            filter=filter,
            fields=[Field(mongo_field="_id", milvus_field="id")],
            search_param=search_param,
            partition_names=[partition_name],
        )

        if not document:
            return

        mongo_result = self.__mongo_collection.update_one(
            {"_id": document["_id"]}, data.mongo_document
        )

        if mongo_result.upserted_id:
            milvus_result = self.__milvus_collection.insert(
                [data.milvus_array],
                partition_name=partition_name,
            )
        elif mongo_result.matched_count:
            milvus_result = self.__update_milvus(
                arrays=[data.milvus_array],
                documents=[document],
                partition_name=partition_name,
                recover=True,
            )
        else:
            return

        if milvus_result is None or not milvus_result.insert_count:
            # Add the old document back in case something went wrong with milvus
            document.pop("milvus_data")
            self.__mongo_collection.update_one({"_id": document["_id"]}, document)
            return

        self.__mongo_collection.update_one(
            filter={"_id": document["_id"]},
            update={"milvus_id": milvus_result[0][0].id},
            upsert=upsert,
        )

    def update_many(
        self,
        data: Document,
        filter: Filter | None = None,
        search_param: dict | None = None,
        partition_name: str | None = None,
        upsert: bool = True,
    ):
        if filter.milvus_filter is None:
            self.__mongo_collection.update_many(
                filter.mongo_filter,
                data.mongo_document,
                upsert=upsert,
            )
            return

        documents = self.find_many(
            filter=filter,
            fields=[Field(mongo_field="_id", milvus_field="id")],
            search_param=search_param,
            partition_names=[partition_name],
        )
        if not documents:
            return

        mongo_filter = {"_id": {"$in": [doc["_id"] for doc in documents]}}
        return self.__mongo_collection.update_many(
            filter=mongo_filter,
            update=data.mongo_document,
            upsert=upsert,
        )

    def delete_one(
        self,
        filter: Filter | None = None,
        partition_name: str | None = None,
    ):
        if filter.milvus_filter is None:
            self.__mongo_collection.delete_one(filter.mongo_filter)
            return

        document = self.find_one(
            filter=filter,
            partition_name=partition_name,
            fields=[Field(mongo_field="_id", milvus_field="id")],
        )

        if not document:
            return

        mongo_result = self.__mongo_collection.delete_one({"_id": document["_id"]})
        if not mongo_result.deleted_count:
            return

        milvus_result = self.__milvus_collection.delete(
            f"id in [{document['milvus_id']}]"
        )
        if not milvus_result.delete_count:
            document.pop("milvus_data")
            self.__mongo_collection.insert_one(document)
            return

    def delete_many(
        self,
        filter: Filter | None = None,
        partition_name: str | None = None,
    ):
        if filter.milvus_filter is None:
            self.__mongo_collection.delete_many(filter.mongo_filter)
            return

        documents = self.find_many(
            filter=filter,
            partition_name=partition_name,
            fields=[Field(mongo_field="_id", milvus_field="id")],
        )

        if not documents:
            return

        mongo_result = self.__mongo_collection.delete_many(
            {"_id": {"$in": [doc["_id"] for doc in documents]}}
        )
        if not mongo_result.deleted_count:
            return

        milvus_filter = [doc["milvus_id"] for doc in documents]
        milvus_result = self.__milvus_collection.delete(
            expr=f"id in [{', '.join(list(map(str, milvus_filter)))}]"
        )
        if not milvus_result.delete_count:
            return

    def distinct(self, key, filter: dict | None = None):
        return self.__mongo_collection.distinct(key=key, filter=filter)

    def drop(self):
        self.__mongo_collection.drop()
        self.__milvus_collection.drop()

    def count(self, filter: dict | None = None):
        return self.__mongo_collection.count_documents(filter)

    def create_indexes(self, indexes: list[Index]):
        mongo_indexes = [asdict(index.mongo_index) for index in indexes]
        milvus_indexes = [
            asdict(milvus_index)
            for index in indexes
            for milvus_index in index.milvus_indexes
        ]

        for mongo_index in mongo_indexes:
            mongo_index = _filter_none(mongo_index)
            mongo_index["background"] = True
            key = (mongo_index.pop("key"), mongo_index.pop("type"))

            self.__mongo_collection.create_index(key, **mongo_index)

        for milvus_index in milvus_indexes:
            milvus_index = _filter_none(milvus_index)
            milvus_index["field_name"] = milvus_index.pop("key")
            if "name" in milvus_index:
                milvus_index["index_name"] = milvus_index.pop("name")

            index_type: dict = milvus_index["index_type"]
            milvus_index["index_params"] = {
                "metric_type": milvus_index["metric_type"],
                "index_type": index_type.pop("name"),
                "index_params": {},
            }
            if index_type:
                milvus_index["index_params"]["params"] = index_type

            self.__milvus_collection.create_index(**milvus_index)

    def drop_indexes(self, indexes: list[DropIndex]):
        for index in indexes:
            if index.mongo_index is not None:
                self.__mongo_collection.drop_index(index.mongo_index)
            if index.milvus_index is not None:
                if not index.milvus_index:
                    self.__milvus_collection.drop_index()
                else:
                    self.__milvus_collection.drop_index(index_name=index.milvus_index)

    # =========== Mongo specific ===========

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

    def __update_milvus(
        self,
        arrays: list,
        documents: list[dict],
        partition_name: str | None = None,
        recover: bool = False,
        upsert: bool = False,
    ):
        milvus_filter = [doc["milvus_id"] for doc in documents]

        milvus_result = self.__milvus_collection.delete(
            expr=f"id in [{', '.join(list(map(str, milvus_filter)))}]"
        )
        if recover and (not upsert and not milvus_result.delete_count):
            # Add the old document back in case something went wrong with milvus
            documents[0].pop("milvus_data")
            self.__mongo_collection.replace_one(
                {"_id": documents[0]["_id"]}, documents[0]
            )
            return

        milvus_result = self.__milvus_collection.insert(arrays, partition_name)
        if recover and not milvus_result.insert_count:
            # Add the old document back in case something went wrong with milvus
            documents[0].pop("milvus_data")
            self.__mongo_collection.update_one(
                {"_id": documents[0]["_id"]}, documents[0]
            )
            self.__milvus_collection.insert(
                [doc["milvus_data"] for doc in documents],
                partition_name=partition_name,
            )
            return

        return milvus_result


def _filter_none(obj: dict) -> dict:
    return {key: val for key, val in obj.items() if key is not None}
