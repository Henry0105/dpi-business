# coding=utf-8

import json
import unittest
from module.test.utils import load_json
from module.test.utils import TestJob
from module.events.mapping import IdMapping, LocationMapping
from module.events.mapping_v2 import IdMappingV3


class IDMappingTest(TestJob):
    def setUp(self):
        TestJob.setUp(self)

    def test_submit(self):
        param = load_json("mapping", "id_mapping1.json")
        cf = IdMapping(param)
        cf.receive(u'1\u0001app')
        msg = {"uuid": "cf_output_uuid", "match_cnt": 10}
        cf.receive(u'2\u0001uuid\u0002' + json.dumps(msg))
        job_status = cf.submit()

        self.assertEqual(job_status.status, 1)
        self.mock_dfs.assert_called_once()
        self.hive_submit.assert_called()
        self.spark.submit.assert_called_once()

        print(json.dumps(cf.job, indent=1))
        input = cf.job['params'][0]['inputs'][0]

        self.assertEqual(input['value'], u'http://10.21.131.11:20101/fs/download?path=demo/测试.tar.gz&module=demo')

    def test_submit_location_device_mapping(self):
        param = load_json("mapping", "location_device_mapping_origin.json")
        cf = LocationMapping(param)
        cf.receive(u'1\u0001app')
        msg = {"uuid": "cf_output_uuid", "match_cnt": 10}
        cf.receive(u'2\u0001uuid\u0002' + json.dumps(msg))
        job_status = cf.submit()

        self.spark.submit.assert_called_once()

        print(json.dumps(cf.job, indent=1))
        input = cf.job['params'][0]['inputs'][0]

        self.assertEqual(input['cityCode'], 'cn1_01')

    def test_submit_v3(self):
        param = load_json("mapping", "id_mapping_v3_sql.json")
        cf = IdMappingV3(param)
        cf.receive(u'1\u0001app')
        msg = {"uuid": "cf_output_uuid", "match_cnt": 10}
        cf.receive(u'2\u0001uuid\u0002' + json.dumps(msg))
        job_status = cf.submit()

        self.assertEqual(job_status.status, 1)
        self.hive_submit.assert_called()
        self.spark.submit.assert_called_once()

        print(json.dumps(cf.job, indent=1))
        input = cf.job['params'][0]['inputs'][0]

        self.assertEqual(input['value'].lower(),
                         "select phone, device from test_database.test.table_name where day = '20190807' and mac is not null desc")

if __name__ == "__main__":
    unittest.main()
