package com.distributedMacPlayground;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;

import java.util.List;


public class GNMF {
    public static void main(String[] args) {
        String hadoopLocation = "hdfs://7d88ca6d3cf1:9000/user/root/";
        String path = hadoopLocation + "soc-buzznet.mtx";
        MatrixObject mo = new MatrixObject(Types.ValueType.FP64, path);
        MatrixCharacteristics mc = new MatrixCharacteristics(101163, 101163);
        MetaDataFormat mdf = new MetaDataFormat(mc, Types.FileFormat.MM);
        mo.setMetaData(mdf);
        MatrixBlock matrixBlock = mo.acquireRead();
        System.out.println(matrixBlock.getNumRows());
        System.out.println(matrixBlock.getNumColumns());

        System.out.println("test");
    }
}
