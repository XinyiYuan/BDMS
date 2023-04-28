example编译过程：

```
// example目录下：
make

cd .. // 切换到GraphLite-0.20目录下
start-graphlite example/PageRankVertex.so Input/facebookcombined_4w Output/out
```

编译自己的文件：

```
// Modify Makefile:
EXAMPLE_ALGOS=your_program
make

cd ..
start-graphlite example/your_program.so Input/facebookcombined_4w Output/out
```

测试过程：

```
export LC_ALL="POSIX"
source /home/bdms/setup/GraphLite-0.20/bin/setenv
./setup-test-part2.sh

rm ./score
./run-test-part2.pl ./score 3_StuID_hw2.cc
```

