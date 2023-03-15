from pymongo.database import Database as MongoDatabase
from pymilvus.client.grpc_handler import GrpcHandler
from pymilvus import Collection as MilvusCollection

from .collection import Collection
from .config import MilvusCollectionConfig

MILVUS_DATABASE = tuple[str, GrpcHandler]


class Database:
    def __init__(
        self,
        mongo_database: MongoDatabase,
        milvus_database: MILVUS_DATABASE,
    ) -> None:
        self.__mongo_database = mongo_database
        self.__milvus_database = milvus_database

    def get_drivers(self) -> tuple[MongoDatabase, MILVUS_DATABASE]:
        return (self.__mongo_database, self.__milvus_database)

    def get_collections(self) -> list[Collection]:
        collections = []

        mongo_collections = set(self.__mongo_database.list_collection_names())
        for milvus_collection in self.__milvus_database[1].list_collections():
            if milvus_collection not in mongo_collections:
                continue

            milvus_collection_impl = MilvusCollection(
                milvus_collection, using=self.__milvus_database[0]
            )
            collections.append(
                Collection(
                    mongo_collection=self.__mongo_database[milvus_collection],
                    milvus_collection=milvus_collection_impl,
                )
            )
            mongo_collections.remove(milvus_collection)

        for mongo_collection in mongo_collections:
            collections.append(
                Collection(
                    mongo_collection=self.__mongo_database[mongo_collection],
                    milvus_collection=None,
                )
            )

        return collections

    def get_collection(self, name: str) -> Collection:
        mongo_collection = self.__mongo_database[name]
        milvus_collection = None
        if name in self.__milvus_database[1].list_collections():
            milvus_collection = MilvusCollection(name, using=self.__milvus_database[0])

        return Collection(
            mongo_collection=mongo_collection,
            milvus_collection=milvus_collection,
        )

    def create_collection(
        self,
        name: str,
        milvus_config: MilvusCollectionConfig | dict | None = None,
    ):
        if milvus_config is not None:
            if isinstance(milvus_config, MilvusCollectionConfig):
                milvus_config = milvus_config.to_dict()
            milvus_config["using"] = self.__milvus_database[0]

            self.__milvus_database[1].create_collection(name, **milvus_config)

        self.__mongo_database.create_collection(name)

    def delete_collection(self, name: str) -> None:
        self.__milvus_database[1].drop_collection(name)
        self.__mongo_database.drop_collection(name)

    @property
    def name(self) -> str:
        return self.__mongo_database.name
