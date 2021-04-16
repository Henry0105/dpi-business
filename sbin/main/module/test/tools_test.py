# coding:utf-8
import os
os.environ.setdefault('MID_ENGINE_ENV', 'dev')
os.environ['DATAENGINE_HOME'] = '../../../../'
from module.tools.cfg import CfgParser
from module.tools.opt import OptionParser

__author__ = 'zhangjt'

import unittest

from module.tools import tar


class TestToolMethods(unittest.TestCase):

    # def test_shell(self):
    #     status = shell.submit("ls -l /tmp/")
    #     self.assertEqual(status, 0)
    #
    #     (status, stdout) = shell.submit_with_stdout('ls /tmp')
    #     self.assertEqual(status, 0)
    #     print(stdout)

    # (status, stderr) = shell.submit_with_stderr('ls -l /tmp/1/1/2/3/3/3/3')
    # self.assertEqual(status, 1)
    # print(stderr)

    def test_tar_gz(self):
        tar.tgz(["../tools", "../test"], "./tmp/1/1/module", "tgz")

    def test_cfg(self):
        os.environ.setdefault('DATAENGINE_HOME', "./")
        os.environ.setdefault('MID_ENGINE_ENV', "dev")
        p = CfgParser()
        v = p.getboolean("b")
        self.assertEqual(v, False)
        v = p.get("dataengine_db_name")
        self.assertEqual(v, "rp_dataengine")
        v = p.getint("max_executors")
        self.assertEqual(v, 40)

    def test_parse_target(self):
        t = OptionParser.parse_target({
            "value": "20180615_354_5b2337cc7fbeeeff431868ae_device.tar.gz",
            "module": "ad_marketing"
        })
        self.assertEqual(t.name, "20180615_354_5b2337cc7fbeeeff431868ae_device")
        self.assertEqual(t.final_path, "20180615_354_5b2337cc7fbeeeff431868ae_device.tar.gz")

        t = OptionParser.parse_target({
            "value": "20180605_402_5b163e4259f4d563ffd49088_device"
        })
        self.assertEqual(t.name, "20180605_402_5b163e4259f4d563ffd49088_device")
        self.assertEqual(t.suffix, "")
        self.assertEqual(t.directory, "./")
        self.assertEqual(t.final_path, "20180605_402_5b163e4259f4d563ffd49088_device")
        t.add_suffix("tar.gz")
        self.assertEqual(t.final_path, "20180605_402_5b163e4259f4d563ffd49088_device")
        self.assertEqual(t.final_name, "20180605_402_5b163e4259f4d563ffd49088_device.tar.gz")
        self.assertEqual(t.suffix, "tar.gz")

        t = OptionParser.parse_target({
            "value": "/20180605_402_5b163e4259f4d563ffd49088_device.tar.gz",
            "description": "dfs目标文件名称"
        })
        self.assertEqual(t.name, "20180605_402_5b163e4259f4d563ffd49088_device")
        self.assertEqual(t.final_path, "/20180605_402_5b163e4259f4d563ffd49088_device.tar.gz")
        self.assertEqual(t.final_name, "20180605_402_5b163e4259f4d563ffd49088_device.tar.gz")

        t = OptionParser.parse_target({
            "value": "/tmp/tmp2/20180605_402_5b163e4259f4d563ffd49088_device.tgz",
            "description": "dfs目标文件名称",
        })
        self.assertEqual(t.name, "20180605_402_5b163e4259f4d563ffd49088_device")
        self.assertEqual(t.suffix, "tgz")
        self.assertEqual(t.directory, "/tmp/tmp2/")
        self.assertEqual(t.final_path, "/tmp/tmp2/20180605_402_5b163e4259f4d563ffd49088_device.tgz")
        self.assertEqual(t.final_name, "20180605_402_5b163e4259f4d563ffd49088_device.tgz")

        t = OptionParser.parse_target({
            "value": "tmp/20180605_402_5b163e4259f4d563ffd49088_device",
            "description": "dfs目标文件名称",
            "suffix": "tgz"
        })
        self.assertEqual(t.name, "20180605_402_5b163e4259f4d563ffd49088_device")
        self.assertEqual(t.suffix, "tgz")
        self.assertEqual(t.final_path, "tmp/20180605_402_5b163e4259f4d563ffd49088_device")
        self.assertEqual(t.final_name, "20180605_402_5b163e4259f4d563ffd49088_device.tgz")

        t = OptionParser.parse_target({
            "module": "ad_marketing",
            "value": "tmp/20180605_402_5b163e4259f4d563ffd49088_device.tar.gz"
        })
        self.assertEqual(t.name, "20180605_402_5b163e4259f4d563ffd49088_device")
        self.assertEqual(t.suffix, "tar.gz")
        self.assertEqual(t.final_path, "tmp/20180605_402_5b163e4259f4d563ffd49088_device.tar.gz")
        self.assertEqual(t.final_name, "20180605_402_5b163e4259f4d563ffd49088_device.tar.gz")
        self.assertEqual(t.file_encrypt_type, 0)
        self.assertEqual(t.file_encrypt_args, {})

    def test_parse_file_encrypt(self):
        t = OptionParser.parse_target({
            "module": "ad_marketing",
            "value": "tmp/20180605_402_5b163e4259f4d563ffd49088_device.tar.gz",
            "fileEncrypt": {
                "encryptType": 1,
                "args": {
                    'pw': 'pw'
                }
            }
        })
        self.assertEqual(t.name, "20180605_402_5b163e4259f4d563ffd49088_device")
        self.assertEqual(t.suffix, "tar.gz")
        self.assertEqual(t.final_path, "tmp/20180605_402_5b163e4259f4d563ffd49088_device.tar.gz")
        self.assertEqual(t.final_name, "20180605_402_5b163e4259f4d563ffd49088_device.tar.gz")
        self.assertEqual(t.file_encrypt_type, 1)
        self.assertEqual(t.file_encrypt_args, {'pw': 'pw'})



if __name__ == '__main__':
    unittest.main()
