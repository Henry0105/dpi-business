# coding=utf-8

import json
import unittest

from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.profile_cal import ProfileCalAppInfo
from module.tools.utils import md5


class AppInfoTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("profile_cal", "profile_cal_app_info2.json")
        appinfo = ProfileCalAppInfo(param)
        appinfo.receive(u'1\u0001app')

        job_status = appinfo.submit()

        self.assertEqual(job_status.status, 0)
        self.mock_dfs.assert_called_once()
        self.hive_submit.assert_called()
        self.spark.submit.assert_called_once()

        input = appinfo.job['params'][0]['inputs'][0]
        output = appinfo.job['params'][0]['output']
        self.assertEqual(input['uuid'], md5("dfs_url"))
        self.assertEqual(output['value'], 'output_dfs_url')
        self.assertEqual(output['uuid'], 'output_uuid')

if __name__ == "__main__":
    unittest.main()
