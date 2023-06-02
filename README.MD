# DistributedMacPlayground

The full name of this repository is Distributed Matrix Computation Playground. As 
the name say, this repository will be used to record some methods of distributed 
matrix computation. 

We will focus on distributed matrix multiplication at first. We employ the matrix
organization from [SystemDS](https://github.com/apache/systemds).

We have simplified the struct in DistributedMacPlayground from SystemDS. We only 
focus on the matrix computation in SystemDS rather than the whole SystemDS (
included instruction parse, compiler optimization, etc.)

## Use in local environment

If you only want to run it in the local machine,  you can find test code which run in local from `src/test/java`.

We use the JUnit to write some test code for every distributed matrix multiplication.

Before you run test code, you should make sure your project have the path `src/test/java/cache/Cpmm`,  `src/test/java/cache/Mapmm`, `src/test/java/cache/PMapmm`, etc. (We will fix it if we have time.)

## Use in cluster environment

You should package the project by using `maven` if you want to run the code in spark cluster. We can run the project from Main class and change the data and different distributed matrix multiplication by changing the corresponded parameter.

You can run the next command to submit the application to the spark cluster.  `--deploy-mode` can be `cluster` or `client`.  The value of   `--master` can be found in the website of your spark cluster. 

```shell
spark-submit --deploy-mode cluster\
             --master spark://6e1929967e39:7077 \
             --class com.distributedMacPlayground.Main \
             [packaged .jar file (you need to make sure it has enough dependencies.)] \
             [main parameters]
```

The example of  full command:

```shell
gangliastart

systemctl start gmond.service && ssh hadoop102 "systemctl start gmond.service" && ssh hadoop103 "systemctl start gmond.service"

sudo systemctl stop gmond.service && ssh hadoop102 "sudo systemctl stop gmond.service" && ssh hadoop103 "sudo systemctl stop gmond.service"
```

```shell
bin/spark-submit --deploy-mode cluster\
             --conf spark.driver.host=hadoop101 \
             --master spark://hadoop101:7077 \
             --class com.distributedMacPlayground.Main \
             DistributeMacPlayground-1.0-SNAPSHOT-jar-with-dependencies.jar \
             -mmtype CPMM \
             -datatype index \
             -in1 hdfs://hadoop101:8020/user/hadoop/100x300x10_matrix_index.csv \
             -in2 hdfs://hadoop101:8020/user/hadoop/300x100x10_matrix_index.csv 
```



```shell
bin/spark-submit --deploy-mode cluster\
             --conf spark.driver.host=hadoop101 \
             --master spark://hadoop101:7077 \
             --class com.distributedMacPlayground.Main \
             DistributeMacPlayground-1.0-SNAPSHOT-jar-with-dependencies.jar \
             -mmtype RMM \
             -datatype data \
             -row 10000 \
             -col 10000 \
             -middle 100000 \
             -blockSize 1000 \
             -in1 hdfs://hadoop101:8020/user/hadoop/10000x100000x0.1.csv \
             -in2 hdfs://hadoop101:8020/user/hadoop/100000x10000x0.1.csv \
             -outputempty true \
             -CACHETYPE left
```

>修改spark 中的conf，添加`spark.local.dir  /tmp/spark-temp` ——解决磁盘溢出的问题
>
>```shell
>spark.local.dir                     /opt/module/spark/tmp
>spark.executor.memory               30925M
>spark.driver.memory                 30925M
>spark.driver.maxResultSize          45G
>spark.driver.cores                  4
>spark.driver.host 	                hadoop101
>spark.ui.enabled                    true
>```
>
>1. 将index物化成hadoop中的数据
>2. 将hadoop放到系统盘中，避免下次再次生成数据
>3. 系统500，额外510

local submit test：

```shell
bin/spark-submit --deploy-mode cluster \
             --master spark://230153b45b98:7077 \
             --class com.distributedMacPlayground.Main \
             DistributeMacPlayground-1.0-SNAPSHOT-jar-with-dependencies.jar \
             -mmtype cpmm \
             -datatype data \
             -row 30000 \
             -col 20000 \
             -middle 1000 \
             -blockSize 1000 \
             -in1 hdfs://230153b45b98:9000/user/root/10Kx100K.csv \
             -in2 hdfs://230153b45b98:9000/user/root/100Kx10K.csv \
             -save hdfs://230153b45b98:9000/user/root
```

GNMF submit test：

```shell
bin/spark-submit --deploy-mode cluster\
             --master spark://hadoop101:7077 \
             --class com.distributedMacPlayground.GNMF \
             DistributeMacPlayground-1.0-SNAPSHOT-jar-with-dependencies.jar \
             -type RMM \
             -path hdfs://hadoop101:8020/user/hadoop/soc-buzznet.mtx \
             -row 101163 \
             -col 101163 \
             -middle 1000 \
             -BLOCKSIZE 1000 \
             -iter 1
```



The main parameters can write by considering the follow table.

| Parameter Name | Type    | Default | Necessary                                            | Description                                                                                                                                                               |
| -------------- | ------- | ------- | ---------------------------------------------------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| -mmType        | String  | NULL    | True                                                 | The method of distributed matrix multiplication. You can choose `CpMM`, `MapMM`, `PMapMM` and `RMM`.                                                                      |
| -dataType      | String  | NULL    | True                                                 | The type of  input  data. You should make sure your input file are  .csv files. The value of this parameter can be `data` or `index`.                                     |
| -in1           | String  | NULL    | True                                                 | The file path of  left matrix.  You must be the `hdfs` type like `hdfs://localhost:9000/[your hdfs path]`                                                                 |
| -in2           | String  | NULL    | True                                                 | The file path of  right matrix.  You must be the `hdfs` type like `hdfs://localhost:9000/[your hdfs path]`                                                                |
| -row           | int     | NULL    | False if `dataType=index` ，True if  `dataType=data` | The output matrix row.                                                                                                                                                    |
| -col           | int     | NULL    | False if `dataType=index` ，True if  `dataType=data` | The output matrix col.                                                                                                                                                    |
| -middle        | int     | NULL    | False if `dataType=index` ，True if  `dataType=data` | The common dimension of the two matrix.                                                                                                                                   |
| -blockSize     | int     | NULL    | False if `dataType=index` ，True if  `dataType=data` | The size of block partitioned from large distributed matrix .                                                                                                             |
| -cacheType     | String  | left    | False                                                | Use in MapMM. The value of this parameter can be `left` or `right`. Broadcast the left matrix if `cacheType=left`. Broadcast the right matrix if `cacheType=right`        |
| -aggType       | String  | multi   | False                                                | Use in MapMM. The value of this parameter can be `multi` or `sigle`.  It means whether the input matrix is only a single block. The input matrix usually is multi blocks. |
| -outputEmpty   | boolean | false   | False                                                | Use in MapMM. Whether can output a empty matrix.                                                                                                                          |
| -min           | double  | 5       | False                                                | Use it when `dataType=index`. The minimum of the generated matrix.                                                                                                        |
| -max           | double  | 10      | False                                                | Use it when `dataType=index`. The maximum of the generated matrix.                                                                                                        |
| -sparsity      | double  | 1       | False                                                | Use it when `dataType=index`. It range in `[0,1]`. The sparsity of the generated matrix.                                                                                  |
| -seed          | int     | -1      | False                                                | Use it when `dataType=index`. The random seed of the generated matrix.                                                                                                    |
| -pdf           | String  | uniform | False                                                | Use it when `dataType=index`.  The random distribution of the generated matrix.  The value of this parameter can be `uniform`, `normal` or `poisson`.                     |

## Format of input file

If `dataType=data`:

Your input file should be a .csv file. For example, the matrix format like $2\times3$  can be expressed by follow format:

>1,2,3
>
>2,3,4

We will use the value as the distributed matrix value.

If `dataType=data`:

The name of your input file should be the format like `[row length]x[col length]x[block size]_matrix_index.csv`, such as `12345x321x10_matrix_index.csv`.

And the content of the file should be the block index. For example, `4x4x2_matrix_index.csv` can be expressed by follow format: (Row can be split to 2 blocks and col can be split to 2 blocks, so it has 2x2=4 index. )

>1,1
>
>1,2
>
>2,1
>
>2,2

We will use the index to generate partitioned distributed matrix.

# If this project is useful for you, please give us a star. Thank you.

## The config of the spark cluster

core-site.xml

```xml
<!-- Put site-specific property overrides in this file. -->

<configuration>
    <!-- 指定 NameNode 的地址 -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop101:8020</value>
    </property>

    <!-- 指定 hadoop 数据的存储目录 -->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/module/hadoop/data</value>
    </property>

    <!-- 配置 HDFS 网页登录使用的静态用户为 atguigu -->
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>hadoop</value>
    </property>
</configuration>
```

hdfs-site.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>
    <!-- nn web 端访问地址-->
    <property>
        <name>dfs.namenode.http-address</name>
        <value>hadoop101:9870</value>
    </property>
    <!-- 2nn web 端访问地址-->
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>hadoop103:50070</value>
    </property>

</configuration>

```



```xml
<?xml version="1.0"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
<configuration>

<!-- Site specific YARN configuration properties -->
<!-- 指定 MR 走 shuffle -->
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>

    <!-- 指定 ResourceManager 的地址-->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>hadoop102</value>
    </property>

    <!-- 环境变量的继承 -->
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CO
        NF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAP
        RED_HOME</value>
    </property>
    <!--是否启动一个线程检查每个任务正使用的物理内存量，如果任务超出分配值，则直接将其杀掉，默认
             是 true -->
    <property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
    </property>

    <property>
        <!--开启日志聚合-->
        <name>yarn.log-aggregation-enable</name>
        <value>true</value>
    </property>
    <property>
        <!--日志聚合hdfs存储路径-->
        <name>yarn.nodemanager.remote-app-log-dir</name>
        <value>/tmp/logs</value>
    </property>
    <property>
        <!--hdfs上的日志保留时间-->
        <name>yarn.log-aggregation.retain-seconds</name>
        <value>604800</value>
    </property>
    <property>
        <name>yarn.nodemanager.local-dirs</name>
        <value>file:/opt/module/local_log/local</value>
    </property>

    <property>
        <!--应用执行时存储路径-->
        <name>yarn.nodemanager.log-dirs</name>
        <value>file:/opt/module/local_log/log</value>
    </property>

    <property>
        <!--应用执行完日志保留的时间，默认0，即执行完立刻删除-->
        <name>yarn.nodemanager.delete.debug-delay-sec</name>
        <value>600</value>
    </property>
</configuration>

```



```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>
    <!-- 指定 MapReduce 程序运行在 Yarn 上 -->
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=/opt/module/hadoop</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/opt/module/hadoop</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/opt/module/hadoop</value>
    </property>
</configuration>

```



