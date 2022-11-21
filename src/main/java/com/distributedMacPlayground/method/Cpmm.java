package com.distributedMacPlayground.method;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.instructions.spark.functions.FilterNonEmptyBlocksFunction;
import org.apache.sysds.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysds.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import scala.Tuple2;

public class Cpmm {

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> execute(JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
                                                                  JavaPairRDD<MatrixIndexes, MatrixBlock> in2,
                                                                  DataCharacteristics mc1, DataCharacteristics mc2) throws Exception {
        if (in1.isEmpty() || in2.isEmpty())
            throw new Exception("Input is empty!");
        if (mc1.getCols() != mc2.getRows())
            throw new Exception("Dimension do not match!");

        int numPreferred = getPreferredParJoin(mc1, mc2, in1.getNumPartitions(), in2.getNumPartitions());
        int numPartJoin = Math.min(getMaxParJoin(mc1, mc2), numPreferred);

        JavaPairRDD<Long, IndexedMatrixValue> tmp1 = in1.mapToPair(new CpmmIndexFunction(true));
        JavaPairRDD<Long, IndexedMatrixValue> tmp2 = in2.mapToPair(new CpmmIndexFunction(false));
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = tmp1.join(tmp2, numPartJoin).mapToPair(new CpmmMultiplyFunction());

        if (mc1.isNoEmptyBlocks() || mc2.isNoEmptyBlocks())
            out = out.filter(new FilterNonEmptyBlocksFunction());
        out = RDDAggregateUtils.sumByKeyStable(out, false);
        return out;
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

    private static class CpmmIndexFunction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, Long, IndexedMatrixValue> {
        private static final long serialVersionUID = -1187183128301671162L;
        private final boolean _left;

        public CpmmIndexFunction(boolean left) {
            _left = left;
        }

        @Override
        public Tuple2<Long, IndexedMatrixValue> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
            IndexedMatrixValue value = new IndexedMatrixValue(arg0._1(), arg0._2());
            Long key = _left ? arg0._1.getColumnIndex() : arg0._1.getRowIndex();
            return new Tuple2<>(key, value);
        }
    }

    private static class CpmmMultiplyFunction implements PairFunction<Tuple2<Long, Tuple2<IndexedMatrixValue, IndexedMatrixValue>>, MatrixIndexes, MatrixBlock> {
        private AggregateBinaryOperator _op = null;

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<Long, Tuple2<IndexedMatrixValue, IndexedMatrixValue>> arg0) throws Exception {
            if (_op == null) {
                _op = InstructionUtils.getMatMultOperator(1);
            }

            MatrixBlock blkIn1 = (MatrixBlock) arg0._2()._1().getValue();
            MatrixBlock blkIn2 = (MatrixBlock) arg0._2()._2().getValue();
            MatrixIndexes ixOut = new MatrixIndexes();

            MatrixBlock blkOut = OperationsOnMatrixValues.matMult(blkIn1, blkIn2, new MatrixBlock(), _op);

            ixOut.setIndexes(arg0._2()._1().getIndexes().getRowIndex(), arg0._2()._2().getIndexes().getColumnIndex());
            return new Tuple2<>(ixOut, blkOut);
        }
    }
}
