from pymongo.database import Database
from pymilvus.client.grpc_handler import GrpcHandler


class Database:
    def __init__(self, mongo_database: Database, milvus_database: GrpcHandler) -> None:
        self.__mongo_database = mongo_database
        self.__milvus_database = milvus_database
