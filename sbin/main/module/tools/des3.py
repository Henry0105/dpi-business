# coding=utf-8

__author__ = 'xdzhang'

import os


def encrypt(input, pass_word, name="", suffix='tar.gz'):
    """
    :文件加密压缩
    :param input: 目标文件或目录
    :param pass_word: 加密密码
    :param name: 文件名字
    :param suffix: 文件后缀,默认tar.gz
    :return:
    """
    if os.path.exists(input) and os.path.isfile(input):
        input_name = os.path.basename(input)
        if name == "":
            out_name = input_name.split('.')[0]
        else:
            out_name = name
    elif os.path.exists(input) and os.path.isdir(input):
        input_name = input
        array = input.split('/')
        if name == "":
            out_name = array[len(array) - 1]
        else:
            out_name = name
    else:
        exit(1)
    os.popen('tar -czPf - %s | openssl des3 -salt -k %s  -out %s.%s'
             % (input_name, pass_word, out_name, suffix))


def decrypt_file(input, pass_word):
    """
    :文件解密解压
    :param input: 目标文件
    :param pass_word: 解密密码
    :return:
    """
    if os.path.exists(input) and os.path.isfile(input):
        os.popen('openssl des3 -d -k %s -salt -in %s | tar xzf -' % (pass_word, input))
    else:
        exit(1)


def decrypt(input, pass_word):
    """
    :文件解密解压
    :param input: 目标文件
    :param pass_word: 解密密码
    :return:
    """
    if not os.path.exists(input):
        exit(1)
    if os.path.isfile(input):
        decrypt_file(input, pass_word)
    elif os.path.isdir(input):
        list = os.listdir(input)
        for i in range(0, len(list)):
            path = os.path.join(input, list[i])
            if os.path.isfile(path):
                decrypt_file(path, pass_word)
    else:
        exit(1)


if __name__ == '__main__':
    decrypt("des", "123456")
