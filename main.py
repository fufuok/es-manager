#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
    main.py
    ~~~~~~~~
    ES 管理脚本

    :author: Fufu, 2021/7/23
    :update: Fufu, 2021/8/21 调整结构, 新建索引时参考昨天的索引而不是今天
    :update: Fufu, 2021/9/18 超时时间增加到 240s, 创建索引失败记录并重试
    :update: Fufu, 2021/11/3 超时时间统一为 300s, 保存最新的 MAPPING, 补齐配置中可能的索引
    :update: Fufu, 2021/11/17 增加新重试机制, 重试 5 轮
    :update: Fufu, 2021/11/18 增加删除重试机制, 重试 5 轮. 不自动创建 Kibana 相关索引
    :update: Fufu, 2021/11/24 删除列表中的索引最长保留时间为 190 天
    :update: Fufu, 2022/04/29 增加提前新建后天的索引
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta
from hashlib import md5

import requests as requests
from elasticsearch import Elasticsearch
from envcrypto import get_environ
from loguru import logger

ROOT_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))
MAPPING_FILE = os.path.join(ROOT_DIR, 'etc', 'all_indices_mapping.json')
INDEX_YMD_FORMAT = '_%y%m%d'
# 创建/删除索引重试次数
MAX_RETRIES = 5
# 默认删除 7 天前的索引
DEFAULT_DAYS = 7
# 未指定天数时, 0 表示 190 天
DEFAULT_DAYS_0 = 190
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
            return Elasticsearch(hosts, timeout=300)
        except Exception as e:
            logger.error('INIT: ({}) {}', i, e)

    exit(1)


def create_new_indexs():
    """新建索引"""
    mapping = load_mapping()
    indices = {}
    today = datetime.now()
    yesterday = today + timedelta(days=-1)
    tomorrow = today + timedelta(days=1)
    after_tomorrow = today + timedelta(days=2)

    # 昨天, 明天, 后天, 索引日志后缀
    suffix_a = yesterday.strftime(INDEX_YMD_FORMAT)
    suffix_b = tomorrow.strftime(INDEX_YMD_FORMAT)
    suffix_c = after_tomorrow.strftime(INDEX_YMD_FORMAT)

    remove_settings = ['uuid', 'provided_name', 'version', 'creation_date']
    retries_indexs = []

    pos = len(suffix_a)
    # 取昨天的索引配置
    for index, conf in ES.indices.get('*' + suffix_a).items():
        index_title = index[:-pos]
        # 移除必要配置项
        for x in remove_settings:
            conf['settings']['index'].pop(x)
        # 保存最新索引配置项
        mapping[index_title] = conf
        indices[index_title] = True
        # 建未来的索引
        for x in [suffix_b, suffix_c]:
            index_x = index_title + x
            if not create_index(index_x, conf):
                retries_indexs.append([index_x, conf])

    # 补漏, 可能存在于待删除列表
    for index_title in get_index_conf():
        if index_title in indices:
            continue
        conf = mapping.get(index_title, {})
        # 建未来的索引
        for x in [suffix_b, suffix_c]:
            index_x = index_title + x
            if not create_index(index_x, conf):
                retries_indexs.append([index_x, conf])

    # 保存最新的 MAPPING
    save_mapping(mapping)

    # 重试创建已失败的索引
    retry_create_index(retries_indexs)


def load_mapping():
    """加载保存的索引配置"""
    try:
        with open(MAPPING_FILE, 'r') as f:
            res = json.load(f)
            return res
    except Exception as e:
        logger.error('LOAD-MAPPING: {}', e)
        return {}


def save_mapping(mapping=None):
    """保存索引配置"""
    try:
        with open(MAPPING_FILE, 'w') as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error('SAVE-MAPPING: {}', e)


def create_index(index_b, conf):
    """建明天的索引"""
    logger.info('NEW-START: {}', index_b)
    time.sleep(1)

    # 先查询索引是否已存在
    if check_index(index_b):
        logger.info('EXISTS: {}', index_b)
        return True

    try:
        res = ES.indices.create(index_b, body=conf, timeout='300s', ignore=400)
        logger.info('NEW-RESULT: {}, OK: {}, [{}] {}', index_b,
                    res.get('acknowledged'), res.get('status'), res.get('error', {}).get('reason', ''))
    except Exception as e:
        logger.error('NEW-ERROR: {} {}', index_b, e)
        time.sleep(60)
        return False

    time.sleep(30)

    creation_date = check_index(index_b)
    if creation_date:
        logger.info('NEW-END: {} {}', index_b, datetime.fromtimestamp(int(creation_date) / 1000).isoformat())
        time.sleep(1)
        return True

    time.sleep(30)
    logger.info('NEW-CHECK: {} NONE', index_b)
    return False


def retry_create_index(retries_indexs):
    """重试创建索引"""
    logger.info("RETRY-INDEXS: {}", retries_indexs)
    for cfg in retries_indexs:
        for m in range(MAX_RETRIES):
            logger.info("RETRY-CREATE({}): {}", m, cfg)
            if create_index(*cfg):
                break


def check_index(index):
    """获取索引创建时间"""
    try:
        creation_date = ES.indices.get(index).get(index, {}). \
            get('settings', {}).get('index', {}).get('creation_date')
        return creation_date if creation_date else 0
    except Exception:
        return 0


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
                    n = DEFAULT_DAYS
                res[index] = n
            return res
    except Exception as e:
        logger.error('CONF: {}', e)
        return {}


def delete_old_indexs():
    """按配置删除旧索引"""
    indexs = get_index_conf()
    for m in range(MAX_RETRIES):
        n = len(indexs)
        if n == 0:
            return
        logger.info("DELETE-INDEX: {}, COUNT: {}", m + 1, n)
        for index in list(indexs.keys()):
            days = indexs[index]
            if days <= 0:
                days = DEFAULT_DAYS_0

            old_index = '{}{}'.format(index, (datetime.now() - timedelta(days)).strftime(INDEX_YMD_FORMAT))
            try:
                ES.indices.delete(old_index, timeout='300s', ignore=[400, 404])
                indexs.pop(index)
                logger.info('DELETE-OK: {}', old_index)
                time.sleep(1)
            except Exception as e:
                logger.error('DELETE-ERROR: {} {}', old_index, e)
                time.sleep(30)


if __name__ == '__main__':
    hosts_main = [
        {'host': '192.168.0.10', 'port': 9200},
        {'host': '192.168.0.11', 'port': 9200},
        {'host': '192.168.0.12', 'port': 9200},
    ]
    hosts_dev = [{'host': '127.0.0.1', 'port': 9200}]

    init_logger()

    logger.info('init es client')
    # ES = init_es(hosts_main)
    ES = init_es(hosts_dev)

    logger.info('create new indexs')
    create_new_indexs()

    logger.info('delete old indexs')
    delete_old_indexs()

    logger.info('done')
