# coding=utf-8
__author__ = 'zhangjt'


class TargetParam:
    def __init__(self, name, suffix, directory, final_path, limit, file_encrypt_type, file_encrypt_args, module="dataengine",
        extension=None):
        self.name = name
        self.directory = directory
        self.suffix = suffix
        self.final_path = final_path
        self.limit = limit
        self.module = module
        self.final_name = "%s.%s" % (name, suffix) if suffix is not "" else name
        # 扩展字段
        self.extension = extension
        self.file_encrypt_type = file_encrypt_type     # int
        self.file_encrypt_args = file_encrypt_args     # dict

    def __str__(self):
        tpl = """TargetParam[
        name={name},final_name={final_name},suffix={suffix},
        directory={directory},final_path={final_path},limit={limit},module={module},
        file_encrypt_args={file_encrypt_args},file_encrypt_type={file_encrypt_type}
        ]"""
        return tpl.format(
            name=self.name,
            final_name=self.final_name,
            suffix=self.suffix,
            directory=self.directory,
            final_path=self.final_path,
            limit=self.limit,
            module=self.module,
            file_encrypt_args=self.file_encrypt_args,
            file_encrypt_type=self.file_encrypt_type
        )

    def add_suffix(self, suffix):
        self.suffix = suffix
        if not self.final_name.endswith("." + self.suffix):
            self.final_name += "." + self.suffix

        # if not self.final_path.endswith("." + self.suffix):
        #     self.final_path += "." + self.suffix


class OptionParser:

    def __init__(self):
        pass

    # 针对dfs target_name
    @staticmethod
    def parse_target(p):
        module = p['module'] if 'module' in p else "dataengine"
        value = str(p['value'])
        limit = p['limit'] if 'limit' in p else 5000000
        encrypt_type = 0
        encrypt_args = {}
        if 'fileEncrypt' in p:
            file_encrypt = p['fileEncrypt']
            encrypt_type = file_encrypt['encryptType'] if 'encryptType' in file_encrypt else 0
            encrypt_args = file_encrypt['args'] if 'args' in file_encrypt else {}

        if str.endswith(value, "tgz"):
            suffix = p['suffix'] if 'suffix' in p else "tgz"
        elif str.endswith(value, "tar.gz"):
            suffix = p['suffix'] if 'suffix' in p else "tar.gz"
        else:
            suffix = p['suffix'] if 'suffix' in p else ""

        arr = value.rsplit("/", 1)

        if len(arr) == 1:
            directory = "."
            name = arr[0].split(".", 1)[0]
        else:
            directory = "/" if arr[0] is "" else arr[0]
            name = arr[1].split(".", 1)[0]

        if not directory.endswith("/"):
            directory += "/"

        # final_path = directory + name + ("." + suffix if suffix is not "" else "")
        final_path = value

        return TargetParam(name=name,
                           final_path=final_path,
                           suffix=suffix,
                           module=module,
                           directory=directory,
                           limit=limit,
                           file_encrypt_type=encrypt_type,
                           file_encrypt_args=encrypt_args
                           )
