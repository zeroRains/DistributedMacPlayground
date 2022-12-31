package com.distributedMacPlayground.operator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.codehaus.janino.Java;

public class Operator {

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> transpose(JavaPairRDD<MatrixIndexes, MatrixBlock> in) {
        // TODO:
        return null;
    }

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> elementWiseDivision(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
            JavaPairRDD<MatrixIndexes, MatrixBlock> in2) {
        // TODO:
        return null;
    }

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> elementWiseMultiply(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
            JavaPairRDD<MatrixIndexes, MatrixBlock> in2) {
        // TODO:
        return null;
    }

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> matrixMultiply(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
            JavaPairRDD<MatrixIndexes, MatrixBlock> in2) {
        // TODO:
        return null;
    }
}
