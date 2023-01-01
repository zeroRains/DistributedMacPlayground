package com.distributedMacPlayground.operator;

import com.distributedMacPlayground.method.MatrixMultiply;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sysds.runtime.functionobjects.Divide;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.SwapIndex;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import scala.Tuple2;

public class Operator {

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> transpose(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in) throws Exception {
        if (in == null) throw new Exception("transpose input is NULL");
        return in.mapToPair(new TransposeFunction());
    }

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> elementWiseDivision(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
            JavaPairRDD<MatrixIndexes, MatrixBlock> in2) throws Exception {
        if (in1 == null || in2 == null) {
            String a = in1 == null ? "left " : "";
            String b = in2 == null ? "right " : "";
            throw new Exception("Element-wise division input " + a + b + "is NULL");
        }
        return in1.join(in2).mapToPair(new ElementWiseFunction(new BinaryOperator(Divide.getDivideFnObject())));
    }

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> elementWiseProduct(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
            JavaPairRDD<MatrixIndexes, MatrixBlock> in2) throws Exception {
        if (in1 == null || in2 == null) {
            String a = in1 == null ? "left " : "";
            String b = in2 == null ? "right " : "";
            throw new Exception("Element-wise product input " + a + b + "is NULL");
        }
        return in1.join(in2).mapToPair(new ElementWiseFunction(new BinaryOperator(Multiply.getMultiplyFnObject())));
    }

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> matrixMultiply(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
            JavaPairRDD<MatrixIndexes, MatrixBlock> in2,
            DataCharacteristics mc1,
            DataCharacteristics mc2,
            MatrixMultiply mm) throws Exception {
        return mm.execute(in1, in2, mc1, mc2);
    }

    private static class TransposeFunction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>,
            MatrixIndexes, MatrixBlock> {
        ReorgOperator rop = new ReorgOperator(SwapIndex.getSwapIndexFnObject());

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<MatrixIndexes, MatrixBlock> arg) throws Exception {
            MatrixBlock data = arg._2().reorgOperations(rop, new MatrixBlock(), 0, 0, 0);
            MatrixIndexes mi = arg._1();
            return new Tuple2<>(new MatrixIndexes(mi.getColumnIndex(), mi.getRowIndex()), data);
        }
    }

    private static class ElementWiseFunction implements PairFunction<Tuple2<MatrixIndexes,
            Tuple2<MatrixBlock, MatrixBlock>>, MatrixIndexes, MatrixBlock> {
        BinaryOperator op;

        public ElementWiseFunction(BinaryOperator op) {
            this.op = op;
        }

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(
                Tuple2<MatrixIndexes, Tuple2<MatrixBlock, MatrixBlock>> pair) throws Exception {
            MatrixIndexes index = pair._1();
            MatrixBlock blk1 = pair._2()._1();
            MatrixBlock blk2 = pair._2()._2();
            MatrixBlock res = blk1.binaryOperations(op, blk2);
            return new Tuple2<>(index, res);
        }
    }
}
