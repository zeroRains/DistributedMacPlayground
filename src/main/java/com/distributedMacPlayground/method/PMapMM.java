package com.distributedMacPlayground.method;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.spark.data.PartitionedBlock;
import org.apache.sysds.runtime.instructions.spark.functions.IsBlockInRange;
import org.apache.sysds.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class PMapMM {

    private static final int NUM_ROWBLOCKS = 4;

    // split the left matrix in row, and use row vector multiply with right matrix.
    public static JavaPairRDD<MatrixIndexes, MatrixBlock> execute(JavaSparkContext sc,
                                                                  JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
                                                                  JavaPairRDD<MatrixIndexes, MatrixBlock> in2,
                                                                  DataCharacteristics mc1) throws Exception {

        StorageLevel storageLevel = StorageLevel.MEMORY_AND_DISK();
        in2 = in2.repartition(sc.defaultParallelism()).persist(storageLevel);

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = null;
        for (int i = 0; i < mc1.getRows(); i += NUM_ROWBLOCKS * mc1.getBlocksize()) {
            JavaPairRDD<MatrixIndexes, MatrixBlock> rdd = in1
                    .filter(new IsBlockInRange(i + 1, i + NUM_ROWBLOCKS * mc1.getBlocksize(), 1, mc1.getCols(), mc1))
                    .mapToPair(new PMapMMRebaseBlockFuction(i / mc1.getBlocksize()));

            int rlen = (int) Math.min(mc1.getRows() - i, NUM_ROWBLOCKS * mc1.getBlocksize());
            PartitionedBlock<MatrixBlock> pmb = SparkExecutionContext.toPartitionedMatrixBlock(rdd, rlen, (int) mc1.getCols(), mc1.getBlocksize(), -1L);
            Broadcast<PartitionedBlock<MatrixBlock>> bpmb = sc.broadcast(pmb);

            JavaPairRDD<MatrixIndexes, MatrixBlock> rdd2 = in2
                    .flatMapToPair(new PMapMMFunction(bpmb, i / mc1.getBlocksize()));
            rdd2 = RDDAggregateUtils.sumByKeyStable(rdd2, false);
            rdd2.persist(storageLevel).count();
            bpmb.unpersist(false);


            if (out == null)
                out = rdd2;
            else
                out = out.union(rdd2);
        }

        out = out.persist(storageLevel);
        out.count();

        return out;
    }

    private static class PMapMMRebaseBlockFuction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> {
        private int _offset = -1;

        public PMapMMRebaseBlockFuction(int i) {
            _offset = i;
        }

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
            long rix = arg0._1().getRowIndex() - _offset;
            MatrixIndexes ixOut = new MatrixIndexes(rix, arg0._1().getColumnIndex());
            return new Tuple2<>(ixOut, arg0._2());
        }
    }

    private static class PMapMMFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> {

        private AggregateBinaryOperator _op = null;
        private Broadcast<PartitionedBlock<MatrixBlock>> _pbc = null;
        private long _offset = -1;

        public PMapMMFunction(Broadcast<PartitionedBlock<MatrixBlock>> bpmb, long i) {
            _pbc = bpmb;
            _offset = i;
            _op = InstructionUtils.getMatMultOperator(1);
        }

        @Override
        public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
            PartitionedBlock<MatrixBlock> pm = _pbc.value();

            MatrixIndexes ixIn = arg0._1();
            MatrixBlock blkIn = arg0._2();


            ArrayList<Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<>();

            for (int i = 1; i <= pm.getNumRowBlocks(); i++) {
                MatrixBlock left = pm.getBlock(i, (int) ixIn.getRowIndex());
                MatrixBlock blkOut = OperationsOnMatrixValues.matMult(left, blkIn, _op);
                ret.add(new Tuple2<>(new MatrixIndexes(_offset + i, ixIn.getColumnIndex()), blkOut));
            }

            return ret.iterator();
        }
    }
}
