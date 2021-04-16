import os
os.environ['MID_ENGINE_ENV'] = 'dev'
print(os.getcwd())
os.environ['DATAENGINE_HOME'] = '../../../../'

from module.tools.dfs import FastDFSHttp


def upload():
    # FastDFSHttp("http://10.5.1.45:20101") \
    #     .upload(
    #     "dataengine/demo/device_crowd_secondary_filter.tar.gz",
    #     "/Users/juntao/src/Yoozoo/dataengine/docs/jobs/filter/device_crowd_secondary_filter.tar.gz",
    #     file_data={
    #         "key": 1
    #     }
    # )
    code = FastDFSHttp().upload(
        "dataengine/tags_demo/20190221_hbase_demo.tar.gz",
        "C:\\Users\\menff\\Documents\\Work\\tasks\\20190221_hbase_demo.tar.gz"
    )
    print(code)


if __name__ == '__main__':
    upload()
    print("go here")
    pass


