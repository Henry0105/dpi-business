# coding=utf-8
import logging
import json

from module.tools import cfg_parser
from module.const import argparse
from enum import Enum, unique
from module.tools.http import HttpClient


@unique
class DataApiStatus(Enum):
    # SUCCESS(0, "成功"),
    # FAILED(-1, "失败"),
    # NO_DATA(0, "没有数据"),
    # NULL_DICT_TYPE(-3, "数据字典类型为空"),
    # WRONG_PARAM(-4, "参数不完整或者参数有误"),
    # REMOVE_TABLE_FAILED(-5, "删除表失败"),
    # UPDATE_TABLE_FAILED(-6,"更新表失败"),
    # ADD_TABLE_FAILED(-7, "添加表失败"),
    # TABLE_ALREADY_EXISTS(-8,"此类型的表已存在"),
    # REMOVE_FAILED(-9, "删除失败"),
    # ADD_FAILED(-10, "添加失败"),
    # UPDATE_FAILED(-11, "修改失败"),
    # OPERATE_FAILED(-12, "操作失败"),
    # NULL_PARAM(-13, "参数不完整或者参数有误");
    SUCCESS = 0
    ADD_TABLE_FAILED = -7
    ADD_FAILED = -10
    UPDATE_FAILED = -11


class DataApiOperator:
    def __init__(self):
        self.url = cfg_parser.get("mob_data_api_url")
        self.key = cfg_parser.get("mob_data_api_key")
        self.api_name = cfg_parser.get("mob_data_api_name")

        if self.url is None:
            raise Exception("must config mob_data_api_url")

    @staticmethod
    def prepare_parser():
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--create_table', action='store_true', default=False, help='创建hbase表')
        parser.add_argument('--add_columns', action='store_true', default=False, help='添加列')
        parser.add_argument('--update_hbase_table_name', action='store_true', default=False, help='修改hbase表名')
        parser.add_argument('--get_cols', action='store_true', default=False, help='获取列名列表')
        parser.add_argument('--apiname', action='store', default=False, help='apiname')
        return parser

    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = DataApiOperator.prepare_parser()
        namespace, other_args = parser.parse_known_args(args=args)

        if namespace.create_table:
            # '状态：1->默认，待审批；2->已撤回；3->审批通过；4->审批未通过；
            # 5->处理中；6->处理成功；7->处理失败；8->测试通过；9->测试未通过'
            self.create_hbase_table(other_args)
        elif namespace.add_columns:
            self.add_table_cols(other_args)
        elif namespace.update_hbase_table_name:
            self.update_hbase_table_name(other_args)
        elif namespace.get_cols:
            self.get_hbase_table_cols()
        else:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()

    # h_data format: "{'name_space':'aa','table_name':'bb','region_num':'cc'}"
    # e.g.: ./dataengine-tools.sh --hbase_table --create_table --table_name test_chenfq --regions 512
    def create_hbase_table(self, args):
        api_path = "mob-data-api/api/hbaseCreateTable"
        full_url = self.url + api_path
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--table_name', action='store', default=False, help='hbase表名, 必须带有namespace')
        parser.add_argument('--regions', action='store', default='512', help='region的个数')
        namespace, argv = parser.parse_known_args(args=args)

        full_table_name = namespace.table_name
        hbase_name_space, table_name = full_table_name.split(':')
        req_body = json.dumps({
            "name_space": hbase_name_space,
            "table_name": table_name,
            "region_num": namespace.regions
        })

        hc = HttpClient()
        r = hc.post(full_url, data={"data": req_body}, timeout=60)
        logging.info(full_url + ", result=>" + r.content)
        status = json.loads(r.content)['status']
        if status == DataApiStatus.SUCCESS.value:
            return True
        else:
            raise Exception(api_path + " failed!")

    def get_hbase_table_cols(self, api_name):
        api_path = "getApiTableCols"
        full_url = self.url + api_path
        hc = HttpClient()
        other_param = "?key={keyparam}&apiName={apiname}".format(keyparam=self.key, apiname=api_name)
        r = hc.post(full_url + other_param, timeout=30)
        logging.info(full_url + ", result=>" + ";".join([item["alias"] for item in json.loads(r.content)["list"]]))
        status = json.loads(r.content)['status']
        if status == DataApiStatus.SUCCESS.value:
            return [item["alias"] for item in json.loads(r.content)["list"]]
        else:
            raise Exception(api_path + " failed!")

    def add_table_cols(self, args):
        api_path = "addApiTableCols"
        full_url = self.url + api_path
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--columns', action='store', default=False, help='hbase列名,带有cf,使用逗号分割')
        parser.add_argument('--apiname', action='store', default=self.api_name, help='apiname,默认是提供给测试的')
        namespace, argv = parser.parse_known_args(args=args)

        columns = namespace.columns.split(',') if len(namespace.columns) > 0 else []
        orig_columns = self.get_hbase_table_cols(namespace.apiname)
        cols_to_add = list(set(columns) - set(orig_columns))

        if len(cols_to_add) > 0:
            other_param = "?key={keyparam}&colNames={cols}&apiName={apiname}".format(
                keyparam=self.key,
                cols=",".join(cols_to_add),
                apiname=namespace.apiname)
            hc = HttpClient()
            r = hc.post(full_url + other_param, timeout=30)
            logging.info(full_url + ", result=>" + r.content)
            status = json.loads(r.content)['status']

            if status == DataApiStatus.SUCCESS.value:
                return True
            else:
                raise Exception(api_path + " failed!")
        else:
            logging.info("none columns to add")

    def update_hbase_table_name(self, args):
        api_path = "updateApiTable"
        full_url = self.url + api_path
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--table_name', action='store', default=False, help='hbase表名, 不需加namespace')
        parser.add_argument('--api_name', action='store', default=False, help='更新表名所需apiname')
        namespace, argv = parser.parse_known_args(args=args)

        hc = HttpClient()
        other_param = "?key={keyparam}&tabName={tabname}&apiName={apiname}".format(
            keyparam=self.key,
            apiname=namespace.api_name,
            tabname=namespace.table_name)

        r = hc.post(full_url + other_param, timeout=30)
        logging.info(full_url + ", result=>" + r.content)
        status = json.loads(r.content)['status']
        if status == DataApiStatus.SUCCESS.value:
            return True
        else:
            raise Exception(api_path + " failed!")
