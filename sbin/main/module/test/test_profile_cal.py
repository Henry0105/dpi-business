# coding=utf-8

import json
import unittest
from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.profile_cal import ProfileCalAppInfo


class ProfileCalTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("profile_cal", "profile_cal_app_info_origin.json")
        cf = ProfileCalAppInfo(param)
        cf.receive(u'1\u0001app')
        msg = {"uuid": "app_info-123123", "match_cnt": 10}
        cf.receive(u'2\u0001uuid\u0002' + json.dumps(msg))
        job_status = cf.submit()

        self.assertEqual(job_status.status, 1)
        self.spark.submit.assert_called_once()

        print(json.dumps(cf.job, indent=1))
        input = cf.job['params'][0]['inputs'][0]

        self.assertEqual(input['uuid'], 'out-123123')


if __name__ == "__main__":
    unittest.main()
