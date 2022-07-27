#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
    main.py
    ~~~~~~~~
    ES 索引迁移

    elasticsearch_reindex forked from borys25ol/elasticsearch-libs
    See: https://github.com/fufuok/elasticsearch_reindex

    :author: Fufu, 2022/7/18
"""
from libs.elasticsearch_reindex import Manager


def main() -> None:
    """
    Example libs function with HTTP Basic authentication.
    """
    config = {
        "source_host": "http://localhost:9201",
        "dest_host": "http://localhost:9202",
        "check_interval": 20,
        "concurrent_tasks": 5,
        "indexes": ["es-index-*"],
        # If the source host requires authentication
        # "source_http_auth": "tmp-source-user:tmp-source-PASSWD.220718",
        # If the destination host requires authentication
        "dest_http_auth": "tmp-reindex-user:tmp--PASSWD.220718",
    }
    manager = Manager.from_dict(data=config)
    manager.start_reindex()


if __name__ == "__main__":
    main()
