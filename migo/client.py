import logging
from typing import Sequence
from pymongo import MongoClient
from pymilvus import connections, Connections

from .config import MongoConfig, MilvusConfig
from .database import Database


class Client:
    def __init__(
        self,
        mongo_config: MongoConfig | dict,
        milvus_config: MilvusConfig | dict,
    ) -> None:
        if isinstance(mongo_config, MongoConfig):
            mongo_config = mongo_config.to_dict(remove_none=True)
        if isinstance(milvus_config, MilvusConfig):
            milvus_config = milvus_config.to_dict(remove_none=True)

        mongo_config["connect"] = True

        self.__mongo_client = MongoClient(**mongo_config)

        try:
            connections.connect(**milvus_config)
        except Exception as e:
            self.__mongo_client.close()
            raise e

    def get_drivers(self) -> tuple[MongoClient, Connections]:
        return (self.__mongo_client, connections)

    def get_databases(self) -> Sequence[Database]:
        databases = []

        mongo_databases = {name for name in self.__mongo_client.list_database_names()}
        for name, client in connections.list_connections():
            if client is not None and name in mongo_databases:
                databases.append(
                    Database(
                        mongo_database=self.__mongo_client.get_database(name),
                        milvus_database=(name, client),
                    )
                )
            elif client is not None:
                try:
                    client.close()
                except Exception as e:
                    logging.error(
                        f"Exception ocurred while cleaning milvus connections:\n{str(e)}"
                    )

        return databases

    def get_database(self, name: str) -> Database:
        milvus_databases = {
            name: client for name, client in connections.list_connections()
        }
        if name not in milvus_databases:
            raise ValueError(f"Database name not found in milvus: {name}")

        return Database(
            mongo_database=self.__mongo_client.get_database(name),
            milvus_database=(name, milvus_databases[name]),
        )

    def get_default_database(self) -> Database:
        milvus_databases = connections.list_connections()
        if not milvus_databases:
            raise ValueError("Milvus has no open connections")

        milvus_alias, milvus_conn = milvus_databases[0]
        if milvus_conn is None:
            raise ValueError("Milvus has no open connections")

        return Database(
            mongo_database=self.__mongo_client.get_database(milvus_alias),
            milvus_database=(milvus_alias, milvus_conn),
        )

    def drop_database(self, name: str) -> None:
        milvus_databases = {
            name: client for name, client in connections.list_connections()
        }
        if name not in milvus_databases:
            raise ValueError(f"Database name not found in milvus: {name}")

        errors = []
        try:
            milvus_databases[name].close()
            connections.remove_connection(name)
        except Exception as e:
            errors.append(e)

        try:
            self.__mongo_client.drop_database(name)
        except Exception as e:
            errors.append(e)

        if errors:
            raise Exception(errors)

    def close(self) -> None:
        errors = []
        for alias, conn in connections.list_connections():
            if conn is not None:
                try:
                    conn.close()
                    connections.remove_connection(alias)
                except Exception as e:
                    errors.append(e)

        try:
            self.__mongo_client.close()
        except Exception as e:
            errors.append(e)

        if errors:
            raise Exception(errors)
