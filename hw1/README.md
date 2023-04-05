Very complicated commands...

I'm pretty bad at writing documents (though I've worked hard), so the following // and /* */ both denote comments (the same syntax as C). I wrote this paragraph in English because I don't think anyone will read this paragraph with Chinese ahead.

```
/*-------docker安装-------*/
// 拉取需要的镜像
docker pull <ImageName>

// 查看本地镜像（包括没在运行的）
docker images

// 大概是安装一下吧，反正下载完先运行一下，而且只用运行一次
docker run -itd <ImageName>

// 查看正在运行的镜像
docker ps

// 改一个好记的名字
docker rename <Old name> <New Name>

/*-------开启新一天的debug^_^-------*/
// 启动docker，这时候$ docker ps就有东西了
docker start hw
docker attach hw

// 启动ssh，dfs，和hbase
service ssh start
start-dfs.sh
start-hbase.sh
// 查看所有运行的结点，应该有七个
jps

// 切换到作业目录
cd /home/bdms/homework/hw1/

// 拷贝input文件夹内的内容到hdfs，这个只需要执行一次
hadoop fs -put ./input /
// 查看hdfs中的文件，最后那个/很重要！
hadoop fs -ls /

// 编译
javac Hw1Grp5.java
// 运行，R=后面是hdfs中的路径而非本地的，参考命令 $ hadoop fs -ls /
java Hw1Grp5 R=/input/distinct_0.tbl select:R0,gt,3 distinct:R1

/*-------验收测试-------*/
cd hw1-check
// 解压检查工具
tar xvzf hw1-check-v1.1.tar.gz

// 抄的ppt
export LC_ALL="POSIX"

// 检查ssh状态
service ssh status
// 检查结点状态，应该有七个
// 不知道为什么我的hbase老是掉线，，掉线了就重新 $ hbase-start.sh 一下
jps

// 这应该是把待测试文件放到hdfs上，具体也不懂
./myprepare

// 检查文件名有无错误，期望输出：Good Files:1 Bad Files:0
./check-group.pl 5_StuID_hw1.java
// 检查能否编译
./check-compile.pl 5_StuID_hw1.java
// 正式运行测试
./run-test.pl ./score 5_StuID_hw1.java
// 测试后生成score，通过cat查看，共3个测试，全部通过则得3分
cat ./score

// 重新测试需要先删除score，再重新运行
rm ./score
./run-test.pl ./score 5_StuID_hw1.java

/*-------下班关灯关门关水电^_^-------*/
// 其实无所谓关不关吧我感觉
stop-hbase.sh
stop-dfs.sh
service ssh stop
exit
```

