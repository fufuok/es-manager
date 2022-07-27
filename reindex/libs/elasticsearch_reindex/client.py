from typing import List, Optional, Tuple

from elasticsearch import Elasticsearch, exceptions

from .const import DEFAULT_ES_KWARGS
from .errors import (
    ES_NODE_NOT_FOUND_ERROR,
    ElasticSearchNodeNotFoundException,
)
from .schema import Config, Index
from .utils import chunkify


class ElasticsearchClient:
    """
    Client for manipulation with Elasticsearch clients and indexes.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.indexes = ",".join(config.indexes) if config.indexes else ""

    @property
    def source_client(self) -> Elasticsearch:
        """
        Return Elasticsearch client where data will be transferred from.
        """
        return self._get_es_client(es_host=self.config.source_host, es_http_auth=self.config.source_http_auth)

    @property
    def dest_client(self) -> Elasticsearch:
        """
        Return Elasticsearch client where data will be transferred.
        """
        return self._get_es_client(es_host=self.config.dest_host, es_http_auth=self.config.dest_http_auth)

    def get_source_indexes(self) -> List[Index]:
        """
        Return all indexes in Elasticsearch and amount of documents.
        """
        indexes = self.source_client.cat.indices(
            h="index,docs.count", s="index",
            index=self.indexes, http_auth=self.config.source_http_auth,
        )
        return self._get_all_indexes(indexes=indexes.split())

    def get_dest_indexes(self) -> List[Index]:
        """
        Return all indexes in Elasticsearch and amount of documents.
        """
        indexes = self.dest_client.cat.indices(
            h="index,docs.count", s="index",
            http_auth=self.config.dest_http_auth,
        )
        return self._get_all_indexes(indexes=indexes.split())

    @staticmethod
    def _get_es_client(es_host: str, es_http_auth: Optional[Tuple[str]] = None) -> Elasticsearch:
        """
        Ping ElasticSearch server and return initialized client object.
        """
        client = Elasticsearch(hosts=es_host, **DEFAULT_ES_KWARGS)  # type: ignore
        try:
            client.info(http_auth=es_http_auth)
        except exceptions.ConnectionError:
            raise ElasticSearchNodeNotFoundException(
                message=ES_NODE_NOT_FOUND_ERROR.format(host=es_host)
            )
        except Exception as e:
            raise ElasticSearchNodeNotFoundException(
                message=ES_NODE_NOT_FOUND_ERROR.format(host='{}, error: {}'.format(es_host, e))
            )
        return client

    @staticmethod
    def _get_all_indexes(indexes: List[str]) -> List[Index]:
        """
        Return all indexes in Elasticsearch and amount of documents.
        """
        return [
            Index(name=name, docs_count=int(count))
            for name, count in chunkify(lst=indexes, n=2)
            if not name.startswith(".")
        ]
