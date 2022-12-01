package com.distributedMacPlayground.method;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysds.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.util.UtilFunctions;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class PMM {
    public static JavaPairRDD<MatrixIndexes, MatrixBlock> execute(JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
                                                                  PartitionedBroadcast<MatrixBlock> in2,
                                                                  DataCharacteristics mc1,  long rlen) throws Exception {
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = in1.flatMapToPair(new RDDPMMFunction(in2, rlen, mc1.getBlocksize()));
        out = RDDAggregateUtils.sumByKeyStable(out, false);
        return out;
    }

    private static class RDDPMMFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> {

        private PartitionedBroadcast<MatrixBlock> _pmV = null;
        private long _rlen = -1;
        private int _blen = -1;

        public RDDPMMFunction(PartitionedBroadcast<MatrixBlock> binput, long rlen, int blen) {
            _blen = blen;
            _rlen = rlen;
            _pmV = binput;
        }

        @Override
        public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
            ArrayList<Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<>();
            MatrixIndexes ixIn = arg0._1();
            MatrixBlock mb2 = arg0._2();

            MatrixBlock mb1 = _pmV.getBlock((int) ixIn.getRowIndex(), 1);

            long minPos = UtilFunctions.toLong(mb1.minNonZero());
            long maxPos = UtilFunctions.toLong(mb1.max());
            long rowIX1 = (minPos - 1) / _blen + 1;
            long rowIX2 = (maxPos - 1) / _blen + 1;
            boolean multipleOuts = (rowIX1 != rowIX2);

            if (minPos >= 1) {
                double spmb1 = OptimizerUtils.getSparsity(mb1.getNumRows(), 1, mb1.getNonZeros());
                long estnnz = (long) (spmb1 * mb2.getNonZeros());
                boolean sparse = MatrixBlock.evalSparseFormatInMemory(_blen, mb2.getNumColumns(), estnnz);

                MatrixBlock out1 = new MatrixBlock();
                MatrixBlock out2 = multipleOuts ? new MatrixBlock() : null;
                out1.reset(_blen, mb2.getNumColumns(), sparse);
                if (out2 != null)
                    out2.reset(UtilFunctions.computeBlockSize(_rlen, rowIX2, _blen), mb2.getNumColumns(), sparse);

                mb1.permutationMatrixMultOperations(mb2, out1, out2);
                out1.setNumRows(UtilFunctions.computeBlockSize(_rlen, rowIX1, _blen));
                ret.add(new Tuple2<>(new MatrixIndexes(rowIX1, ixIn.getColumnIndex()), out1));
                if (out2 != null)
                    ret.add(new Tuple2<>(new MatrixIndexes(rowIX2, ixIn.getColumnIndex()), out2));
            }
            return ret.iterator();
        }
    }
}







