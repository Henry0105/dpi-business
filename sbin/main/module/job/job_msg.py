# coding=utf-8
import json
import unittest


# {
#     "job_id": "id_mapping_1",
#     "job_name": "id_mapping",
#     "error_msg": "",
#     "details": [{
#         "uuid": "4a56e06f-b991-46bf-9643-f1b9fc407582",
#         "match_cnt": "54595154",
#         "id_cnt": 5,
#         "upload_and_cnt": 1,
#         "out_cnt": {
#             "imei_cnt": 1000,
#             "device_cnt": 300
#         },
#         "upload_ios_cnt": 4,
#         "match_ios_cnt": 3
#     }]
# }

class JobMsg:
    DETAILS = "details"

    def __init__(self):
        self.data = {}

    def put(self, path, value):
        """
        将value写入path指定的路径中,支持单个key和带顺序的key的数组
        :param path: 'a' or ['a', 'b']
        :param value: 要写入的内容
        :return:
        """
        if isinstance(path, list):
            res = self.data
            for e in path[:-1]:
                if e not in res:
                    res[e] = {}
                res = res[e]

            res[path[-1]] = value
        else:
            self.data[path] = value

    def merge(self, d):
        for k, v in d.iteritems():
            self.data[k] = v

    def get(self, path):
        """
        将path指定的路径中的数据拿出来,支持单个key和带顺序的key的数组
        :param path: 'a' or ['a', 'b']
        :return:
        """
        if isinstance(path, list):
            res = self.data
            for e in path:
                if isinstance(res, dict):
                    if e in res:
                        res = res[e]
                    else:
                        return None
                else:
                    return None
            return res
        else:
            if path in self.data:
                return self.data[path]
            else:
                return None

    def put_into_details(self, info):
        """
        将info的内容放入details数组中
        :param info:
        :return:
        """
        if JobMsg.DETAILS not in self.data:
            self.data[JobMsg.DETAILS] = []

        tmp = info.copy()
        self.data[JobMsg.DETAILS].append(tmp)

    def get_by_uuid(self, uuid):
        """
        通过uuid去查出对应的array中的数据
        :param uuid:
        :return: dict
        """
        if JobMsg.DETAILS in self.data:
            return next((out for out in self.data[JobMsg.DETAILS] if out['uuid'] == uuid), None)
        else:
            return None

    def is_empty(self):
        return not bool(self.data)

    def has_key(self, key):
        return key in self.data

    def __str__(self):
        return json.dumps(self.data)

    def to_json(self):
        return self.data


class TestJobMsg(unittest.TestCase):
    def test_put(self):
        msg = JobMsg()
        msg.put(['a', 'b'], 1)
        self.assertEqual(msg.data, {'a': {'b': 1}})

    def test_merge(self):
        msg = JobMsg()
        d = {'a': 1, 'b': 2}
        msg.merge(d)
        self.assertEqual(msg.get('a'), d['a'])
        self.assertTrue(msg.has_key('a'))

    def test_get(self):
        msg = JobMsg()
        msg.put(['a', 'b'], 1)
        self.assertEqual(msg.get('a'), {'b': 1})
        self.assertEqual(msg.get(['a', 'b']), 1)
        self.assertEqual(msg.get(['a', 'c']), None)
        self.assertEqual(msg.get(['a', 'b', 'c']), None)
        self.assertEqual(JobMsg().get('a'), None)

    def test_get_by_uuid(self):
        msg = JobMsg()
        out1 = {
            "uuid": "uuid1",
            "match_cnt": 12
        }
        out2 = {
            "uuid": "uuid2",
            "match_cnt": 2
        }
        msg.put_into_details(out1)
        msg.put_into_details(out2)
        self.assertEqual(msg.get_by_uuid("uuid1"), out1)
        self.assertEqual(msg.get_by_uuid("uuid2"), out2)
        self.assertEqual(msg.get_by_uuid("not_exists"), None)

    def test_job_msg_to_json(self):
        msg = JobMsg()
        msg.put_into_details({"uuid": "uuid1", "match_cnt": 2})
        print(json.dumps(msg.to_json()))


if __name__ == "__main__":
    unittest.main()