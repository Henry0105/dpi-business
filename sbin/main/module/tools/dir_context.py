# coding=utf-8
import os
import shutil


class DirContext:
    """
    需要去特定的目录进行读写操作,自动管理目录的创建和删除
    在with上使用
    """
    def __init__(self, target_dir):
        self.cwd = os.getcwd()
        self.target_dir = target_dir

    def __enter__(self):
        if os.path.exists(self.target_dir):
            shutil.rmtree(self.target_dir)

        os.makedirs(self.target_dir)
        os.chdir(self.target_dir)
        print(os.getcwd())

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self.cwd)
        shutil.rmtree(self.target_dir)


if __name__ == "__main__":
    with DirContext('target/dir'):
        print(os.getcwd())
        assert os.path.join('target', os.sep, 'dir') in os.getcwd()
        print("go dir")
    shutil.rmtree('target')
