# coding=utf-8

import json
import unittest

from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.profile_cal import ProfileCalGroup


class ProfileCalGroupTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("profile_cal", "profile_cal_group_origin.json")
        appinfo = ProfileCalGroup(param)
        appinfo.receive(u'1\u0001app')

        job_status = appinfo.submit()

        self.assertEqual(job_status.status, 1)
        self.spark.submit.assert_called_once()

        print(json.dumps(appinfo.job, indent=1))

        input = appinfo.job['params'][0]['inputs'][0]
        output = appinfo.job['params'][0]['output']
        self.assertEqual(input['uuid'], 'bbee788ffea84bc48b6a11a4d6bedcbf')
        self.assertEqual(output['value'], 'tmp/1561378080_location_cfq')
        self.assertEqual(output['uuid'], '5ce5f3f7bc404fd2a0ff02f193518ee4_cfq')

if __name__ == "__main__":
    unittest.main()
