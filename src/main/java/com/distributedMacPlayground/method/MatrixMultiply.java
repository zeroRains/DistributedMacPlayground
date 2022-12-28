package com.distributedMacPlayground.method;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;

public interface MatrixMultiply {

    abstract JavaPairRDD<MatrixIndexes, MatrixBlock> execute(JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
                                                             JavaPairRDD<MatrixIndexes, MatrixBlock> in2,
                                                             DataCharacteristics mc1, DataCharacteristics mc2)throws Exception;
}
