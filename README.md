# 中间件相关发布历史
[release note](http://c.mob.com/display/datacloud/tags)

# 注意

- utils统一入口:dataengine-commons/dataengine/commons/utils
- core中json类必须继承JobInterface
- 异常管理 ExitCode


dataengine-core 整理
    constant ==> 移到 commons
    other ==> 移到 dataengine-utils
    profilecal => 区分开来
    profile
        cal
            calculation=>precision(自定义群体画像精确)
            estimation(自定义群体画像预估)
            module(商业地理中的)
        export
            bt(回溯)


bin