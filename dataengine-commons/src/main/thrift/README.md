## 自动生成代码
当前目录执行

### windows
thrift-0.9.2.exe -r --gen py -out ../../../../../src/main/module/rpc handler.thrift 
thrift-0.9.2.exe -r --gen java -out ../java handler.thrift 

### unix
thrift -r --gen py -out ../../../../../src/main/module/rpc handler.thrift 
thrift -r --gen java -out ../java handler.thrift 


