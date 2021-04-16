# coding=utf-8
import unittest
import os
os.environ.setdefault('MID_ENGINE_ENV', 'dev')
os.environ['DATAENGINE_HOME'] = '../../../../../'
from module.tools.utils import build_encrypt_json


class TestParseEncrypt2NewFormat(unittest.TestCase):
    def test_parse_not_encrypt(self):
        self.assertEqual(build_encrypt_json({})['encrypt_type'], 0)
        self.assertEqual(build_encrypt_json({'encrypt_type': {}})['encrypt_type'], 0)
        self.assertEqual(build_encrypt_json({'encrypt_type': {'value': 0}})['encrypt_type'], 0)

    def test_parse_md5(self):
        self.assertEqual(build_encrypt_json({'encrypt_type': {'value': 1}})['encrypt_type'], 1)

    def test_parse_aes(self):
        old_param = {
            'encrypt_type': {
                'value': 2
            },
            'aes_info': {
                'key': 'key',
                'iv': 'iv'
            }
        }
        new_param = build_encrypt_json(old_param)
        self.assertEqual(new_param['encrypt_type'], 2)
        self.assertEqual(new_param['args'], {'key': 'key', 'iv': 'iv'})


if __name__ == "__main__":
    unittest.main()
