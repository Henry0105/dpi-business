# coding=utf-8
import os


# 跳过测试
def skip_test(func):
    def wrap(self, *args, **kwargs):
        if 'local' == os.environ['MID_ENGINE_ENV']:
            pass
        else:
            return func(self, *args, **kwargs)
    return wrap
