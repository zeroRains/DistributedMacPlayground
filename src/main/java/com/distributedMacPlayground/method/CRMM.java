package com.distributedMacPlayground.method;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysds.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import scala.Tuple2;

public class CRMM {
    public static JavaPairRDD<MatrixIndexes, MatrixBlock> execute(JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
                                                                  JavaPairRDD<MatrixIndexes, MatrixBlock> in2,
                                                                  DataCharacteristics mc1, DataCharacteristics mc2) throws Exception {
        int numPreferred = getPreferredParJoin(mc1, mc2, in1.getNumPartitions(), in2.getNumPartitions());
        int numPartJoin = Math.min(getMaxParJoin(mc1, mc2), numPreferred);

        JavaPairRDD<Integer, Tuple2<Integer, MatrixBlock>> newIn1 = in1.mapToPair(new CRMMMapFunction(true));
        JavaPairRDD<Integer, Tuple2<Integer, MatrixBlock>> newIn2 = in2.mapToPair(new CRMMMapFunction(false));
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = newIn1.join(newIn2,numPartJoin).mapToPair(new CRMMMultiplyMapFunction());
        out = RDDAggregateUtils.sumByKeyStable(out, false);
        return out;
    }


    private static class CRMMMapFunction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, Integer, Tuple2<Integer, MatrixBlock>> {
        boolean left;

        public CRMMMapFunction(boolean b) {
            left = b;
        }

        @Override
        public Tuple2<Integer, Tuple2<Integer, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> args) throws Exception {
            int rowIndex = (int) args._1().getRowIndex();
            int colIndex = (int) args._1().getColumnIndex();
            return left ? new Tuple2<>(colIndex, new Tuple2<>(rowIndex, args._2())) :
                    new Tuple2<>(rowIndex, new Tuple2<>(colIndex, args._2()));
        }
    }

    private static class CRMMMultiplyMapFunction
            implements PairFunction<Tuple2<Integer, Tuple2<Tuple2<Integer, MatrixBlock>,
            Tuple2<Integer, MatrixBlock>>>, MatrixIndexes, MatrixBlock> {
        private AggregateBinaryOperator _op = null;

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<Integer,
                Tuple2<Tuple2<Integer, MatrixBlock>, Tuple2<Integer, MatrixBlock>>> args) throws Exception {
            if (_op == null) {
                _op = InstructionUtils.getMatMultOperator(1);
            }
            int rowIndex = args._2()._1()._1();
            int colIndex = args._2()._2()._1();
            MatrixBlock in1 = args._2()._1()._2();
            MatrixBlock in2 = args._2()._2()._2();
            MatrixBlock res = OperationsOnMatrixValues.matMult(in1, in2, new MatrixBlock(), _op);
            return new Tuple2<>(new MatrixIndexes(rowIndex, colIndex), res);
        }
    }

    private static int getPreferredParJoin(DataCharacteristics m1, DataCharacteristics m2, int numPart1, int numPart2) {
//        int defPar = SparkExecutionContext.getDefaultParallelism(true);
        int maxParIn = Math.max(numPart1, numPart2);
        int maxSizeIn = SparkUtils.getNumPreferredPartitions(m1) + SparkUtils.getNumPreferredPartitions(m2);
        int tmp = (m1.dimsKnown(true) && m2.dimsKnown(true)) ? Math.max(maxSizeIn, maxParIn) : maxParIn;
        return tmp;
    }

    private static int getMaxParJoin(DataCharacteristics mc1, DataCharacteristics mc2) {
        return mc1.colsKnown() ? (int) mc1.getNumColBlocks() : mc2.rowsKnown() ? (int) mc2.getNumColBlocks() : Integer.MAX_VALUE;
    }
}
