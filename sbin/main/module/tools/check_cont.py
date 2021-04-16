# coding=utf-8
from enum import unique, Enum


class CheckCont:
    """
    Check的内容:
    参数校验和报错信息的完善
    和上下文的处理
    """
    def __init__(self, params):
        self.params = params
        self.rep = set()
        self.inputs = []
        self.output = []

    def __enter__(self):
        for idx, param in enumerate(self.params):
            for input_idx, input_ in enumerate(param['inputs']):
                self.inputs.append(input_)
            self.output.append(param['output'])
        return self.inputs, self.output

    def __exit__(self, exc_type, exc_val, exc_tb):
        if '' in self.rep:
            self.rep.remove('')
        if len(self.rep) is not 0:
            raise Exception('json参数错误:' + ';'.join(self.rep))

    """参数校验的具体实现"""
    def check(self, list_, arg, types, flag):
        header = '[inputs-{0}]' if flag == 0 else '[output-{0}]'
        _rep = set()
        for input_ in list_:
            # 参数校验实现逻辑:
            # type是枚举类CheckType
            for type_ in types:
                if type_.name == 'EXISTS':
                    if arg not in input_:
                        _rep.add('必须存在')
                elif type_.name == 'NON_EMPYT':
                    if (arg in input_ and len(input_[arg])) == 0 or (arg not in input_):
                        _rep.add('不能为空')
        # 报错信息的输出格式化
        res = '' if len(_rep) is 0 else (header.format(arg) + '且'.join(_rep))
        self.rep.add(res)

    def check_inputs(self, arg, types):
        self.check(self.inputs, arg, types, 0)

    def check_output(self, arg, types):
        self.check(self.output, arg, types, 1)

@unique
class CheckType(Enum):
    EXISTS = 1
    NON_EMPYT = 2
