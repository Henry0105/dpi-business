# coding=utf-8

import json
import unittest

from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.profile_cal import ProfileCalFrequency
from module.tools.utils import md5


class AppInfoTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("profile_cal", "profile_cal_frequency_33369.json")
        appinfo = ProfileCalFrequency(param)
        appinfo.receive(u'1\u0001app')

        job_status = appinfo.submit()

        self.assertEqual(job_status.status, 0)
        self.mock_dfs.assert_called_once()
        self.hive_submit.assert_called()
        self.spark.submit.assert_called_once()

        input = appinfo.job['params'][0]['inputs'][0]
        output = appinfo.job['params'][0]['output']
        self.assertEqual(input['uuid'], 'c4f9664df50cfd89876705cb23062db0')
        self.assertEqual(output['value'], 'toolplat/dev/51a2c56756af4fdf890dad8b2ad5cfa4.tar.gz')
        self.assertEqual(output['uuid'], '20190619142300_6df59d417a9641f1b2d1ba8655432f50')

if __name__ == "__main__":
    unittest.main()
