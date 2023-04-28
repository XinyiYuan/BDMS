编译过程：

```
start-dfs.sh
start-yarn.sh

rm -f *.class *.jar
javac Hw2Part1.java
jar cfm Hw2Part1.jar Hw2Part1-manifest.txt Hw2Part1*.class
hdfs dfs -rm -f -r /hw2/part1-output
hadoop jar Hw2Part1.jar /part1-input/input_0 /hw2/part1-output
hadoop fs -ls /hw2/part1-output
hdfs dfs -cat '/hw2/part1-output/part-*'
```

测试过程：


```
cd hw2-check/
export LC_ALL="POSIX"
./myprepare
./run-test-part1.pl ./score 3_StuID_hw2.java
rm ./score
cat ./score
```

