package com.distributedMacPlayground;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

import java.util.List;


public class GNMF {
    public static void main(String[] args) {
        String hadoopLocation = "hdfs://10.1.1.1:19000/user/root/";
        String path = hadoopLocation + "soc-buzznet.mtx";
        MatrixObject mo = new MatrixObject(Types.ValueType.FP64,path);
        MatrixBlock matrixBlock = mo.acquireRead();
        System.out.println(matrixBlock.getNumRows());
        System.out.println(matrixBlock.getNumColumns());

        System.out.println("test");
    }
}
