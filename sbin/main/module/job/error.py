# coding=utf-8
import unittest


class DataEngineException(Exception):
    ERROR_DICT = {
        "输入数据为空": ["insertions must be positive", "shutdown hook called before final status was reported"],
        "输入数据过大": ["spark.kryoserializer.buffer.max", "spark.driver.maxresultsize",
                   "spark.executor.memoryoverhead"],
        "输入数据有误": ["malformedurlexception", "400 for url"],
        "输入任务参数有误":["参数错误"]
    }
    DEFAULT_ERROR_MSG = "任务运行报错,请稍后重试"

    def __init__(self, raw_error_msg, error_msg = None):
        """
        :param raw_error_msg: 实际的异常信息
        :param error_msg: 给外部看的异常信息(经过加工的)
        """
        self.raw_error_msg = raw_error_msg.lower()
        if error_msg is None:
            self.error_msg = DataEngineException.wrap_error_msg(self.raw_error_msg)
        else:
            self.error_msg = error_msg.lower()

    def to_dict(self):
        return {'error_msg': self.error_msg, 'raw_error_msg': self.raw_error_msg}

    def __str__(self):
        return "%s(%s)" % (self.error_msg, self.raw_error_msg)

    @staticmethod
    def wrap_error_msg(raw_msg):
        for error_msg, kws in DataEngineException.ERROR_DICT.iteritems():
            if DataEngineException.msg_includes(raw_msg.lower(), kws):
                return error_msg
        return DataEngineException.DEFAULT_ERROR_MSG

    @staticmethod
    def msg_includes(msg, kws):
        return any([kw in msg for kw in kws])


class DataEngineExceptionTest(unittest.TestCase):
    def test_msg_includes(self):
        self.assertTrue(DataEngineException.msg_includes('a b c', ['a']))
        self.assertTrue(DataEngineException.msg_includes('a b c', ['d', 'a']))
        self.assertFalse(DataEngineException.msg_includes('a b c', ['d']))

    def test_wrap_error_msg(self):
        self.assertEqual(DataEngineException.wrap_error_msg(" insertions must be positive "), "输入数据为空")
        self.assertEqual(DataEngineException.wrap_error_msg(" insertions must be good "),
                         DataEngineException.DEFAULT_ERROR_MSG)

    def test_none_exception(self):
        print(str(DataEngineException("test")))
        print(str(None))


if __name__ == "__main__":
    unittest.main()