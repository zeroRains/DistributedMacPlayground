# DistributedMacPlayground

The full name of this repository is Distributed Matrix Computation Playground. As 
the name say, this repository will be used to record some methods of distributed 
matrix computation. 

We will focus on distributed matrix multiplication at first. We employ the matrix
organization from [SystemDS](https://github.com/apache/systemds).

We have simplified the struct in DistributedMacPlayground from SystemDS. We only 
focus on the matrix computation in SystemDS rather than the whole SystemDS (
included instruction parse, compiler optimization, etc.)

You can submit your jar package to spark cluster by following code
```shell
spark-submit --deploy-mode cluster --master spark://6e1929967e39:7077 --class com.distributedMacPlayground.Main DistributeMacPlayground-1.0-SNAPSHOT-jar-with-dependencies.jar CpMM in1.csv in2.csv
```

**If this project is useful for you, please give us a star. Thank you.**