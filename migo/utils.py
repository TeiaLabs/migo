from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Generic, Literal, Optional, TypeVar, Union

import pymongo

F = TypeVar("F", bound=Callable[..., Any])


class copy_sig(Generic[F]):
    def __init__(self, target: F) -> None:
        ...

    def __call__(self, wrapped: Callable[..., Any]) -> F:
        ...


class IndexType(Enum):
    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING
    GEO2D = pymongo.GEO2D
    GEOSPHERE = pymongo.GEOSPHERE
    HASHED = pymongo.HASHED
    TEXT = pymongo.TEXT


@dataclass
class FlatIndex:
    name: str = "FLAT"


@dataclass
class BINFlatIndex:
    name: str = "BIN_FLAT"


@dataclass
class IVFIndex:
    nlist: int


@dataclass
class BINIVFIndex(IVFIndex):
    name: str = "BIN_IVF_FLAT"


@dataclass
class IVFFlatIndex(IVFIndex):
    name: str = "IVF_FLAT"


@dataclass
class IVFSQ8Index(IVFIndex):
    name: str = "IVF_SQ8"


@dataclass
class IVFPQIndex(IVFIndex):
    m: float
    nbits: int
    name: str = "IVF_PQ"


@dataclass
class HNSWINdex:
    M: int
    efConstruction: int
    name: str = "HNSW"


@dataclass
class AnnoyIndex:
    n_trees: int
    name: str = "ANNOY"


@dataclass
class DISKANNIndex:
    name: str = "DISKANN*"


@dataclass
class MilvusFloatingIndex:
    key: str
    metric_type: Literal["L2", "IP"]
    index_type: Union[
        FlatIndex,
        IVFFlatIndex,
        IVFSQ8Index,
        IVFPQIndex,
        HNSWINdex,
        AnnoyIndex,
        DISKANNIndex,
    ]
    name: Optional[str] = None


@dataclass
class MilvusBinaryIndex:
    key: str
    metric_type: Literal[
        "JACCARD",
        "TANIMOTO",
        "HAMMING",
        "SUPERSTRUCTURE",
        "SUBSTRUCTURE",
    ]
    index_type: Union[BINFlatIndex, BINIVFIndex]
    name: Optional[str] = None


@dataclass
class MongoIndex:
    key: str
    name: Optional[str] = None
    unique: bool = False
    type: IndexType = IndexType.ASCENDING
    sparse: bool = False
    expiration_secs: Optional[int] = None
    hidden: Optional[bool] = None


@dataclass
class MongoGeoIndex(MongoIndex):
    bucket_size: Optional[int] = None
    min: Optional[float] = None
    max: Optional[float] = None


@dataclass
class DropIndex:
    mongo_index: Optional[str] = None
    milvus_index: Optional[str] = None


@dataclass
class Field:
    mongo_field: str
    milvus_field: str


@dataclass
class Filter:
    mongo_filter: dict | None = None
    milvus_filter: dict[str, list] | None = None


@dataclass
class Document:
    mongo_document: dict
    milvus_array: list | None = None


@dataclass
class BatchDocument:
    mongo_documents: list[dict]
    milvus_arrays: list[list] | None = None


@dataclass
class Index:
    mongo_index: Union[MongoIndex, MongoGeoIndex]
    milvus_index: Union[
        MilvusFloatingIndex,
        MilvusBinaryIndex,
    ]
