package com.distributedMacPlayground.method;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysds.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysds.runtime.matrix.data.TripleIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import scala.Tuple2;

import javax.ws.rs.core.Link;
import java.util.Iterator;
import java.util.LinkedList;

public class Rmm {
    public static JavaPairRDD<MatrixIndexes, MatrixBlock> execute(JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
                                                                  JavaPairRDD<MatrixIndexes, MatrixBlock> in2,
                                                                  DataCharacteristics mc1, DataCharacteristics mc2) throws Exception {
        JavaPairRDD<TripleIndexes, MatrixBlock> tmp1 = in1.flatMapToPair(new RmmReplicateFunction(mc2.getCols(), mc2.getBlocksize(), true));
        JavaPairRDD<TripleIndexes, MatrixBlock> tmp2 = in2.flatMapToPair(new RmmReplicateFunction(mc1.getRows(), mc1.getBlocksize(), false));
        DataCharacteristics mcOut = new MatrixCharacteristics(mc1.getRows(), mc2.getCols(), mc1.getBlocksize());

        int numPartJoin = getNumJoinPartitions(mc1, mc2);
        int numParOut = SparkUtils.getNumPreferredPartitions(mcOut);
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = tmp1.join(tmp2, numPartJoin).mapToPair(new RmmMultiplyFunction());
        out = RDDAggregateUtils.sumByKeyStable(out, numParOut, false);
        return out;
    }

    private static int getNumJoinPartitions(DataCharacteristics mc1, DataCharacteristics mc2) {
        double hdfsBlockSize = InfrastructureAnalyzer.getHDFSBlockSize();
        double matrix1PSize = OptimizerUtils.estimatePartitionedSizeExactSparsity(mc1)
                * ((long) Math.ceil((double) mc2.getCols() / mc2.getBlocksize()));
        double matrix2PSize = OptimizerUtils.estimatePartitionedSizeExactSparsity(mc2)
                * ((long) Math.ceil((double) mc1.getCols() / mc1.getBlocksize()));
        return (int) Math.max(Math.ceil((matrix1PSize + matrix2PSize) / hdfsBlockSize), 1);
    }

    private static class RmmReplicateFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, TripleIndexes, MatrixBlock> {
        private long _len = -1;
        private long _blen = -1;
        private boolean _left = false;

        public RmmReplicateFunction(long _len, long _blen, boolean _left) {
            this._len = _len;
            this._blen = _blen;
            this._left = _left;
        }

        @Override
        public Iterator<Tuple2<TripleIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
            LinkedList<Tuple2<TripleIndexes, MatrixBlock>> ret = new LinkedList<>();
            MatrixIndexes ixIn = arg0._1();
            MatrixBlock blkIn = arg0._2();
            long numBlock = (long) Math.ceil((double) _len / _blen);
            if (_left) {
                long i = ixIn.getRowIndex();
                long k = ixIn.getColumnIndex();
                for (long j = 1; j <= numBlock; j++) {
                    TripleIndexes tmptrix = new TripleIndexes(i, j, k);
                    ret.add(new Tuple2<>(tmptrix, blkIn));
                }
            } else {
                long k = ixIn.getRowIndex();
                long j = ixIn.getColumnIndex();
                for (long i = 1; i <= numBlock; i++) {
                    TripleIndexes tmptrix = new TripleIndexes(i, j, k);
                    ret.add(new Tuple2<>(tmptrix, blkIn));
                }
            }
            return ret.iterator();
        }
    }

    private static class RmmMultiplyFunction implements PairFunction<Tuple2<TripleIndexes, Tuple2<MatrixBlock, MatrixBlock>>, MatrixIndexes, MatrixBlock> {
        private AggregateBinaryOperator _op = null;

        public RmmMultiplyFunction() {
            AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
            _op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
        }

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<TripleIndexes, Tuple2<MatrixBlock, MatrixBlock>> arg0) throws Exception {
            TripleIndexes ixIn = arg0._1();
            MatrixIndexes key = new MatrixIndexes(ixIn.getFirstIndex(), ixIn.getSecondIndex());
            MatrixBlock in1 = arg0._2()._1();
            MatrixBlock in2 = arg0._2()._2();

            MatrixBlock value = OperationsOnMatrixValues.matMult(in1, in2, new MatrixBlock(), _op);

            return new Tuple2<>(key, value);
        }
    }
}
