# coding=utf-8
import json
import unittest
from module.test.utils import TestJob
from module.test.utils import load_json
from module.events.edd_utils import EddUtils


class EddUtilsTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("profile_cal","edd_utils.json")
        cf = EddUtils(param)
        job_status = cf.submit()
        self.assertEqual(job_status.status, 1)
        print(json.dumps(cf.job, indent=1))
        input = cf.job['params'][0]['inputs'][0]
        output = cf.job['params'][0]['output']
        self.assertEqual(output['hdfsOutput'], '/tmp/dataengine/schedule/data_check/0')

if __name__ == "__main__":
    unittest.main()
