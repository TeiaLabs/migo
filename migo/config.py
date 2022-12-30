from bson.codec_options import TypeRegistry
from dataclasses import dataclass, field
from typing import Literal, Callable, Sequence, Union, Any

from dataclasses_json import config, dataclass_json, DataClassJsonMixin
from pymongo.server_description import ServerDescription
from pymongo.monitoring import (
    CommandListener,
    ServerListener,
    TopologyListener,
    ConnectionPoolListener,
    ServerHeartbeatListener,
)
from pymilvus import CollectionSchema
from pymongo.encryption_options import AutoEncryptionOpts


EVENT_LISTENERS = Union[
    CommandListener,
    ServerListener,
    TopologyListener,
    ConnectionPoolListener,
    ServerHeartbeatListener,
]
SERVER_SELECTOR_CALLABLE = Callable[[list[ServerDescription]], list[ServerDescription]]
UNICODE_ERROR_HANDLER_OPTIONS = Literal[
    "strict", "replace", "backslashreplace", "surrogateescape", "ignore"
]
READ_PREFERENCE_OPTIONS = Literal[
    "primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"
]
AUTH_MECHANISMS_OPTIONS = Literal[
    "DEFAULT",
    "GSSAPI",
    "MONGODB-AWS",
    "MONGODB-CR",
    "MONGODB-X509",
    "PLAIN",
    "SCRAM-SHA-1",
    "SCRAM-SHA-256",
]


@dataclass_json
class BaseConfig:
    def to_dict(self, remove_none: bool = False) -> dict:
        out = DataClassJsonMixin.to_dict(self)
        if remove_none:
            return {key: value for key, value in out.items() if value is not None}

        return out


@dataclass
class MilvusCollectionConfig(BaseConfig):
    schema: CollectionSchema
    shards_num: int | None = None
    consistency_level: str | int | None = None


@dataclass(frozen=True, slots=True)
class MongoConfig(BaseConfig):
    host: str | None = None
    port: int | None = None
    tz_aware: bool | None = None
    type_registry: TypeRegistry | None = None
    datetime_conversion: Literal[
        "datetime", "datetime_auto", "datetime_clamp"
    ] = "datetime"
    direct_connection: bool = field(
        default=False, metadata=config(field_name="directConnection")
    )
    max_pool_size: int = field(default=0, metadata=config(field_name="maxPoolSize"))
    max_idle_time: int | None = field(
        default=None, metadata=config(field_name="maxIdleTimeMS")
    )
    max_connections: int = field(default=2, metadata=config(field_name="maxConnecting"))
    timeout: int = field(default=0, metadata=config(field_name="timeoutMS"))
    socket_timeout: int = field(
        default=0, metadata=config(field_name="socketTimeoutMS")
    )
    connect_timeout: int = field(
        default=20000, metadata=config(field_name="connectTimeoutMS")
    )
    server_selector: SERVER_SELECTOR_CALLABLE | None = None
    server_selection_timeout: int = field(
        default=30000, metadata=config(field_name="serverSelectionTimeoutMS")
    )
    wait_queue: int | None = field(
        default=None, metadata=config(field_name="waitQueueTimeoutMS")
    )
    heartbeat_frequency: int = field(
        default=10000, metadata=config(field_name="heartbeatFrequencyMS")
    )
    appname: str = "migo"
    event_listeners: Sequence[EVENT_LISTENERS] = field(default_factory=list)
    retry_writes: bool = field(default=True, metadata=config(field_name="retryWrites"))
    retry_reads: bool = field(default=True, metadata=config(field_name="retryReads"))
    unicode_decode_error_handler: UNICODE_ERROR_HANDLER_OPTIONS = "strict"
    min_replication: int | None = field(default=None, metadata=config(field_name="w"))
    min_replication_timeout: int | None = field(
        default=None, metadata=config(field_name="wTimeoutMS")
    )
    wait_for_journal: bool | None = field(
        default=None, metadata=config(field_name="journal")
    )
    wait_for_fsync: bool | None = field(
        default=None, metadata=config(field_name="fsync")
    )
    replica_set_name: str | None = field(
        default=None, metadata=config(field_name="replicaSet")
    )
    read_preference: READ_PREFERENCE_OPTIONS = "primary"
    read_preference_tags: str | None = field(
        default=None, metadata=config(field_name="readPreferenceTags")
    )
    max_staleness_seconds: int = field(
        default=-1, metadata=config(field_name="maxStalenessSeconds")
    )
    username: str | None = None
    password: str | None = None
    auth_source: str | None = field(
        default=None, metadata=config(field_name="authSource")
    )
    auth_mechanism: AUTH_MECHANISMS_OPTIONS | None = field(
        default=None, metadata=config(field_name="authMechanism")
    )
    auth_mechanism_properties: Any = None
    tls: bool = False
    tls_insecure: bool = field(default=False, metadata=config(field_name="tlsInsecure"))
    tls_allow_invalid_certificate: bool = field(
        default=False, metadata=config(field_name="tlsAllowInvalidCertificates")
    )
    tls_allow_invalid_hostnames: bool = field(
        default=False, metadata=config(field_name="tlsAllowInvalidHostnames")
    )
    tls_certificate_path: str | None = field(
        default=None, metadata=config(field_name="tlsCAFile")
    )
    tls_certificate_key_path: str | None = field(
        default=None, metadata=config(field_name="tlsCertificateKeyFile")
    )
    tls_certificate_revocation_list_path: str | None = field(
        default=None, metadata=config(field_name="tlsCRLFile")
    )
    tls_certificate_key_password_path: str | None = field(
        default=None, metadata=config(field_name="")
    )
    tls_disable_ocsp_check: bool = field(
        default=False, metadata=config(field_name="tlsDisableOCSPEndpointCheck")
    )
    read_concern_level: str | None = field(
        default=None, metadata=config(field_name="readConcernLevel")
    )
    auto_encryption_options: AutoEncryptionOpts | None = None


@dataclass(frozen=True, slots=True)
class MilvusConfig(BaseConfig):
    alias: str | None = None
    address: str | None = None
    uri: str | None = None
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    secure: bool = False
    client_key_path: str | None = None
    client_pem_path: str | None = None
    ca_pem_path: str | None = None
    server_pem_path: str | None = None
    server_name: str | None = None
