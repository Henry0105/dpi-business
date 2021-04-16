# coding=utf-8

from module.const.argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('-v', '--version', action='version', version="1.0")
parser.add_argument('-create_hive_table', '--create_hive_table', action='store_true', default=False,
                    help='创建dataengine库表,目前创建通过运维')
parser.add_argument('-upload_dfs', '--upload_dfs', action='store_true', default=False, help='上传文件')
parser.add_argument('-device_full_tags2hfile', '--device_full_tags2hfile', action='store_true',
                    default=False, help='全量标签导入hbase')
parser.add_argument('-idmapping', '--idmapping', action='store_true', default=False)
print parser.parse_known_args(['-device'])
print parser.parse_known_args(['-device_full_tags2hfile'])
