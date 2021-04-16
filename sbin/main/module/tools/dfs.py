# coding=utf-8
import json
import logging
import urllib
import os
import shutil
import time

from module.const import argparse
from module.tools import tar
from module.tools import des3
from module.tools.env import dataengine_env
from module.tools.http import HttpClient
from module.tools.utils import singleton
from module.tools.opt import OptionParser
from module.tools.cfg import prop_utils
from module.tools.hive import Hive
from module.job.error import DataEngineException
from module.tools import shell

__author__ = 'zhangjt'



@singleton
class FastDFSHttp:
    """
    @see: http://c.mob.com/pages/viewpage.action?pageId=5666888
    """
    SUCCESS = "200"

    def __init__(self):
        self.dfs_root_url = dataengine_env.dfs_root_url

    # @see TargetParam
    def tgz_upload(self, target_param):
        if target_param.suffix is None or target_param.suffix is "":
            target_param.add_suffix("tar.gz")

        logging.info("target_param=>%s", target_param)
        try:
            if target_param.file_encrypt_type is 0:
                tar.tgz([target_param.name], target_param.name, target_param.suffix)
            elif target_param.file_encrypt_type is 1:
                des3.encrypt(target_param.name, target_param.file_encrypt_args['pw'],
                             target_param.name, target_param.suffix)
            status = self.upload(
                path=target_param.final_path,
                target_file=target_param.final_name,
                module=target_param.module,
                file_data=target_param.extension,
            )
            return status
        except Exception, e:
            raise DataEngineException("上传文件失败", e.message)

    def upload(self, path, target_file, user_id="system", module='dataengine', overwrite=True, file_data=None):
        url = '%s/fs/upload' % self.dfs_root_url
        d = {
            'module': module,
            'userId': user_id,
            'path': path,
            'overwrite': overwrite,
            'expireDays': 60,
            'append': False,
        }
        if file_data is not None:
            d['fileData'] = json.dumps(file_data)

        for i in range(3):
            try:
                r = HttpClient().post(url, d, target_file)
                ret_code = json.loads(r.text)["code"]
                if ret_code == FastDFSHttp().SUCCESS:
                    return self.upload_success(r, url, target_file, d, path, module)
                else:
                    logging.info("ret is: %s" % r.text)
                    time.sleep(3)

            except Exception, e:
                logging.error('Failed to post: ' + str(e))
                time.sleep(3)

        raise DataEngineException(raw_error_msg="上传文件失败")

    def upload_success(self, response, url, target_file, data, path, module):
        logging.info('target_file => %s' % target_file)
        logging.info('%s => %s' % (url, json.dumps(data, indent=1)))
        logging.info('%s/fs/download?path=%s&module=%s' % (self.dfs_root_url, path, module))
        logging.info(response.text)
        return response.status_code

    def upload2(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--dfs_path', help='dfs路径')
        parser.add_argument('--local_path', help='本地路径')
        namespace, other_args = parser.parse_known_args(args=args)
        if other_args:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()
        self.upload(namespace.dfs_path, namespace.local_path)

    def download(self, path, target, module='dataengine'):
        url = '%s/fs/download?path=%s&module=%s' % (self.dfs_root_url, path, module)
        testfile = urllib.URLopener()
        testfile.retrieve(url, target)

    def download_with_decrypt_with_input(self, input, target_name):
        self.download_with_decrypt(input['value'], input.get('compress_type', None), target_name, input.get('fileEncrypt', None))

    def download_with_decrypt_with_input_v2(self, input, target_name):
        self.download_with_decrypt(input['value'], input.get('compressType', None), target_name, input.get('fileEncrypt', None))

    def download_with_decrypt(self, url, compression, target_name, file_encrypt):
        testfile = urllib.URLopener()
        url_encode = url.encode('utf-8')
        try:
            testfile.retrieve(url_encode, target_name)
        except BaseException, e:
            print(e.message)
            raise DataEngineException('json[inputs]参数错误:dfs地址错误')
        if file_encrypt is not None and file_encrypt['fileEncryptType'] == 1:
            des3.decrypt(target_name, file_encrypt['args']['pw'])
        elif compression is None or len(compression.strip()) < 1 or compression.strip().lower() == "none" :
            pass
        else:  # gz
            gz_name = '%s.tar.gz' % target_name
            shell.submit_with_stdout("mv {target_name} {gz_name}".format(target_name=target_name, gz_name=gz_name))
            if os.path.exists(target_name):
                if os.path.isdir(target_name):
                    shutil.rmtree(target_name)
                else:
                    os.remove(target_name)
            os.makedirs(target_name)
            shell.submit_with_stdout("tar -xzvf %s -C %s" % (gz_name, target_name))


    def upload_from_hive_cache(self, output, where, data_dir, target_dir):
        limit = output['limit'] if 'limit' in output else None
        query = """INSERT OVERWRITE LOCAL DIRECTORY '{dir}'
                   SELECT data
                   FROM {target_table}
                   {where_clause}
                   {limit_clause}
            """.format(where_clause="" if where is None else "where %s" % where,
                       dir=data_dir,
                       limit_clause="" if limit is None else "limit %s" % str(limit),
                       target_table=prop_utils.HIVE_TABLE_DATA_OPT_CACHE)

        self.upload_from_hive_query(output, target_dir, query)

    def upload_from_hive_query(self, output, target_dir, query):
        target_param = OptionParser.parse_target(output)
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)

        os.makedirs(target_dir)
        os.chdir(target_dir)

        status = Hive().submit(query)

        if status is not 0:
            raise DataEngineException('上传文件失败', "hdfs download failed")

        fast_dfs.tgz_upload(target_param)

    def tgz_upload_files(self, files, target_param):
        import copy
        if target_param.suffix is None or target_param.suffix is "":
            target_param.add_suffix("tar.gz")
        for fullname in files:
            filename = fullname.replace('/', '_')
            filename = "%s.%s" % (filename, target_param.suffix) \
                if target_param.suffix is not "" else filename
            target_param_copy = copy.copy(target_param)
            target_param_copy.name = fullname
            target_param_copy.final_name = "%s.%s" % (fullname, target_param.suffix)\
                if target_param.suffix is not "" else fullname
            target_param_copy.final_path = "%s/%s" % (target_param.final_path, filename)
            self.tgz_upload(target_param_copy)


fast_dfs = FastDFSHttp()
