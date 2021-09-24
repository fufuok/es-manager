#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
    main.py
    ~~~~~~~~
    ES 管理脚本

    :author: Fufu, 2021/7/23
    :update: Fufu, 2021/8/21 调整结构, 新建索引时参考昨天的索引而不是今天
    :update: Fufu, 2021/9/18 超时时间增加到 240s, 创建索引失败记录并重试
"""
import os
import sys
import time
from datetime import datetime, timedelta

import requests as requests
from elasticsearch import Elasticsearch
from envcrypto import get_environ
from loguru import logger

ROOT_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))
ES = None


def init_logger():
    """日志初始化"""
    logger.remove()
    logger.add(
        sys.stderr,
        level='DEBUG',
        format='<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | '
               '<level>{level}</level> | '
               '<level>{message}</level>',
    )
    logger.add(
        os.path.join(ROOT_DIR, 'log', 'run.log'),
        rotation='00:00',
        retention='10 days',
        compression='zip',
        format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}',
        enqueue=True,
    )


def init_es(hosts):
    """连接 ES"""
    for i in range(5):
        try:
            return Elasticsearch(hosts, timeout=180)
        except Exception as e:
            logger.error('INIT: ({}) {}', i, e)

    exit(1)


def create_new_indexs():
    """新建索引"""
    today = datetime.now()
    yesterday = today + timedelta(days=-1)
    tomorrow = today + timedelta(days=1)
    suffix = {yesterday.strftime(x): tomorrow.strftime(x) for x in ['_%y%m%d', '.%m.%d']}

    remove_settings = ['uuid', 'provided_name', 'version', 'creation_date']
    retries_indexs = []

    for a, b in suffix.items():
        pos = len(a)
        # 取昨天的索引配置
        for index, conf in ES.indices.get('*' + a).items():
            # 移除必要配置项
            for x in remove_settings:
                conf['settings']['index'].pop(x)
            index_b = index[:-pos] + b
            # 建明天的索引
            if not create_index(index_b, conf):
                retries_indexs.append([index_b, conf])

    for x in retries_indexs:
        create_index(*x)


def create_index(index_b, conf):
    """建明天的索引"""
    try:
        ES.indices.create(index_b, body=conf, timeout='240s', ignore=400)
    except Exception as e:
        logger.error('NEW: {} {}', index_b, e)
        time.sleep(120)
        return False

    # 索引创建时间
    creation_date = ES.indices.get(index_b).get(index_b, {}). \
        get('settings', {}).get('index', {}).get('creation_date')
    logger.info('NEW: {} {}', index_b,
                datetime.fromtimestamp(int(creation_date) / 1000).isoformat() if creation_date else 'None')
    time.sleep(30)
    return True


def get_index_conf():
    """获取索引配置文件"""
    # 获取数据源待删除索引配置
    group_name = 'es-delete-old-index'
    conf_file = os.path.join(ROOT_DIR, 'etc', '{}.conf'.format(group_name))
    api_key = get_environ('ESM_XY_MONITOR_API_KEY', group_name)
    if api_key:
        # 获取远程配置
        api_url = 'http://demo.conf-center.com/api' \
                  'name={}&demo_token={}'.format(group_name, api_key)
        try:
            resp = requests.get(api_url).json()
            if resp['ok'] == 1:
                # 最新配置
                with open(conf_file, 'w+', encoding='utf-8', newline='\n') as f:
                    f.write(resp['data'][0]['ip_info'])
        except Exception as e:
            logger.error('CONF: {} {}', group_name, e)

    # 加载索引配置文件
    try:
        with open(conf_file, 'r', encoding='utf-8') as f:
            res = {}
            for conf in f.readlines():
                conf = conf.strip()
                if not conf or conf.startswith('#'):
                    continue
                conf += ' '
                index, n = conf.split(' ', 1)
                try:
                    n = int(n.strip())
                except Exception:
                    n = 7
                res[index] = n
            return res
    except Exception as e:
        logger.error('CONF: {}', e)
        return {}


def delete_old_indexs():
    """按配置删除旧索引"""
    for index, n in get_index_conf().items():
        if n > 0:
            old_index = '{}_{}'.format(index, (datetime.now() - timedelta(days=n)).strftime('%y%m%d'))
            try:
                ES.indices.delete(old_index, timeout='240s', ignore=[400, 404])
            except Exception as e:
                logger.error('DELETE: {} {}', old_index, e)
                time.sleep(60)
                continue

            logger.info('DELETE: {}', old_index)
            time.sleep(10)


if __name__ == '__main__':
    hosts_es = [
        {'host': '192.168.0.10', 'port': 9200},
        {'host': '192.168.0.11', 'port': 9200},
        {'host': '192.168.0.12', 'port': 9200},
    ]
    # hosts_dev = [{'host': '1.2.3.4', 'port': 9200}]

    init_logger()

    logger.info('init es client')
    # ES = init_es(hosts_dev)
    ES = init_es(hosts_es)

    logger.info('create new indexs')
    create_new_indexs()

    logger.info('delete old indexs')
    delete_old_indexs()

    logger.info('done')
