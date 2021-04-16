# coding=utf-8
import json

__author__ = 'zhangjt'

import logging
import os
import sys
import time

import requests
import requests.adapters

if sys.version_info.major < 3:
    from urllib import url2pathname
else:
    from urllib.request import url2pathname


class LocalFileAdapter(requests.adapters.BaseAdapter):
    """Protocol Adapter to allow Requests to GET file:// URLs
    @todo: Properly handle non-empty hostname portions.
    """

    @staticmethod
    def _chkpath(method, path):
        """Return an HTTP status for the given filesystem path."""
        if method.lower() in ('put', 'delete'):
            return 501, "Not Implemented"  # TODO
        elif method.lower() not in ('get', 'head'):
            return 405, "Method Not Allowed"
        elif os.path.isdir(path):
            return 400, "Path Not A File"
        elif not os.path.isfile(path):
            return 404, "File Not Found"
        elif not os.access(path, os.R_OK):
            return 403, "Access Denied"
        else:
            return 200, "OK"

    def send(self, req, **kwargs):  # pylint: disable=unused-argument
        """Return the file specified by the given request

        @type req: C{PreparedRequest}
        @todo: Should I bother filling `response.headers` and processing
               If-Modified-Since and friends using `os.stat`?
        """
        path = os.path.normcase(os.path.normpath(url2pathname(req.path_url)))
        response = requests.Response()

        response.status_code, response.reason = self._chkpath(req.method, path)
        if response.status_code == 200 and req.method.lower() != 'head':
            try:
                response.raw = open(path, 'rb')
            except (OSError, IOError) as err:
                response.status_code = 500
                response.reason = str(err)

        if isinstance(req.url, bytes):
            response.url = req.url.decode('utf-8')
        else:
            response.url = req.url

        response.request = req
        response.connection = self

        return response

    def close(self):
        pass


class HttpClient:
    def __init__(self):
        self.requests_session = requests.session()
        self.requests_session.mount('file://', LocalFileAdapter())

    def get(self, url, retry_num=3, timeout=None):
        ret_code = None
        for i in range(retry_num):
            try:
                r = self.requests_session.get(url, timeout=timeout)
                r.encoding = 'utf-8'
                ret_code = r.status_code
                if ret_code is not 200:
                    logging.info("ret_code %s, with retry: %s" % (ret_code, str(i)))
                    time.sleep(3)
                else:
                    return r.text
            except Exception as e:
                logging.error('Failed to get: %s with url %s' % (str(e), url))
                time.sleep(3)

        raise Exception("get error %s with %s" % (ret_code, url))

    def post(self, url, data=None, target_file=None, retry_num=3, timeout=None, json=None, headers=None):
        ret_code = None
        for i in range(retry_num):
            try:
                if target_file is not None:
                    r = requests.post(
                        url, data=data, files={'file': open(target_file, "rb")},
                        timeout=timeout, json=json, headers=headers)
                else:
                    r = requests.post(url, data=data, timeout=timeout, json=json, headers=headers)
                ret_code = r.status_code
                if ret_code is not 200:
                    logging.info("ret_code %s, content: %s with retry: %d" % (ret_code, r.content, i))
                    time.sleep(3)
                else:
                    return r
            except Exception as e:
                logging.error('Failed to post: ' + str(e))
                time.sleep(3)

        raise Exception("post error %s with %s" % (ret_code, url))

    def put(self, url, data = None, json=None, target_file=None, retry_num=3, timeout=None):
        ret_code = None
        for i in range(retry_num):
            try:
                if target_file is not None:
                    r = requests.put(url, data=data, json=json, files={'file': open(target_file, "rb")}, timeout=timeout)
                else:
                    r = requests.put(url, data=data, json=json, timeout=timeout)
                ret_code = r.status_code
                if not ret_code in range(200, 208):
                    logging.info("ret_code %s, with retry: %d" % (ret_code, i))
                    time.sleep(3)
                else:
                    return r
            except Exception as e:
                logging.error('Failed to put: ' + str(e))
                time.sleep(3)

        raise Exception("put error %s with %s" % (ret_code, url))


if __name__ == '__main__':
    a = HttpClient().get("http://bd041-041.yzdns.com:10880/ws/v1/cluster/apps/application_1531196858550_2602", 3)
    b = json.loads(a)
    print(str(b['app']['diagnostics']))
