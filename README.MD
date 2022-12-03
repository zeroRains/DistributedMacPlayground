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

You should package the project by using `maven` if you want to run the code. We can run the project from Main class and change the data and different distributed matrix multiplication by changing the corresponded parameter.

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
spark-submit --deploy-mode cluster\
             --master spark://6e1929967e39:7077 \
             --class com.distributedMacPlayground.Main \
             DistributeMacPlayground-1.0-SNAPSHOT-jar-with-dependencies.jar \
             -mmtype MapMM \
             -datatype index \
             -in1 hdfs://localhost:9000/user/root/test/100x300x10_matrix_index.csv \
             -in2 hdfs://localhost:9000/user/root/test/300x100x10_matrix_index.csv
```

The main parameters can write by considering the follow table.

| Parameter Name | Type    | Default | Necessary                                            | Description                                                  |
| -------------- | ------- | ------- | ---------------------------------------------------- | ------------------------------------------------------------ |
| -mmType        | String  | NULL    | True                                                 | The method of distributed matrix multiplication. You can choose `CpMM`, `MapMM`, `PMapMM` and `RMM`. |
| -dataType      | String  | NULL    | True                                                 | The type of  input  data. You should make sure your input file are  .csv files. The value of this parameter can be `data` or `index`. |
| -in1           | String  | NULL    | True                                                 | The file path of  left matrix.  You must be the `hdfs` type like `hdfs://localhost:9000/[your hdfs path]` |
| -in2           | String  | NULL    | True                                                 | The file path of  right matrix.  You must be the `hdfs` type like `hdfs://localhost:9000/[your hdfs path]` |
| -row           | int     | NULL    | False if `dataType=index` ，True if  `dataType=data` | The output matrix row.                                       |
| -col           | int     | NULL    | False if `dataType=index` ，True if  `dataType=data` | The output matrix col.                                       |
| -middle        | int     | NULL    | False if `dataType=index` ，True if  `dataType=data` | The common dimension of the two matrix.                      |
| -blockSize     | int     | NULL    | False if `dataType=index` ，True if  `dataType=data` | The size of block partitioned from large distributed matrix . |
| -cacheType     | String  | left    | False                                                | Use in MapMM. The value of this parameter can be `left` or `right`. Broadcast the left matrix if `cacheType=left`. Broadcast the right matrix if `cacheType=right` |
| -aggType       | String  | multi   | False                                                | Use in MapMM. The value of this parameter can be `multi` or `sigle`.  It means whether the input matrix is only a single block. The input matrix usually is multi block. |
| -outputEmpty   | boolean | false   | False                                                | Use in MapMM. Whether can output a empty matrix.             |
| -min           | double  | 5       | False                                                | Use it when `dataType=index`. The minimum of the generated matrix. |
| -max           | double  | 10      | False                                                | Use it when `dataType=index`. The maximum of the generated matrix. |
| -sparsity      | double  | 1       | False                                                | Use it when `dataType=index`. It range in `[0,1]`. The sparsity of the generated matrix. |
| -seed          | int     | -1      | False                                                | Use it when `dataType=index`. The random seed of the generated matrix. |
| -pdf           | String  | uniform | False                                                | Use it when `dataType=index`.  The random distribution of the generated matrix.  The value of this parameter can be `uniform`, `normal` or `poisson`. |

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