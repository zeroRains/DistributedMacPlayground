package com.distributedMacPlayground.method;

import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.CommonConfig.SparkAggTye;
import com.distributedMacPlayground.CommonConfig.CacheTpye;
import com.distributedMacPlayground.util.ExecutionUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.spark.data.LazyIterableIterator;
import org.apache.sysds.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysds.runtime.instructions.spark.functions.FilterNonEmptyBlocksFunction;
import org.apache.sysds.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaData;
import scala.Tuple2;

import java.util.Iterator;
import java.util.stream.IntStream;

public class Mapmm {

    public static MatrixBlock execute(JavaSparkContext sc, MatrixBlock in1, MatrixBlock in2, int blen,
                                      CacheTpye type, SparkAggTye _aggType) throws Exception {
        return execute(sc, in1, in2, blen, type, _aggType, true);
    }

    // format a x b
    public static MatrixBlock execute(JavaSparkContext sc, MatrixBlock a, MatrixBlock b, int blen,
                                      CacheTpye type, SparkAggTye _aggType, boolean outputEmpty) throws Exception {
        MatrixBlock rdd = type.isRight() ? a : b;
        MatrixBlock bcast = type.isRight() ? b : a;
        DataCharacteristics mcRdd = new MatrixCharacteristics(rdd.getNumRows(), rdd.getNumColumns(), blen, rdd.getNonZeros());
        DataCharacteristics mcBc = new MatrixCharacteristics(bcast.getNumRows(), bcast.getNumColumns(), blen, bcast.getNonZeros());

        int numParts = (requiresFlatMapFunction(type, mcBc) && requiresRepartitioning(type, mcRdd, mcBc,
                sc.defaultMinPartitions())) ? getNumRepartitioning(type, mcRdd, mcBc) : -1;
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, rdd, blen, numParts, outputEmpty);

        if (requiresFlatMapFunction(type, mcBc) &&
                requiresRepartitioning(type, mcRdd, mcBc, in1.getNumPartitions())) {
            int numParts1 = getNumRepartitioning(type, mcRdd, mcBc);
            int numParts2 = getNumRepartitioning(type.getFilped(), mcBc, mcRdd);
            if (numParts2 > numParts1) {
                type = type.getFilped();
                rdd = type.isRight() ? a : b;
                bcast = type.isRight() ? b : a;
                mcRdd = new MatrixCharacteristics(rdd.getNumRows(), rdd.getNumColumns(), blen, rdd.getNonZeros());
                mcBc = new MatrixCharacteristics(bcast.getNumRows(), rdd.getNumColumns(), blen, rdd.getNonZeros());
                in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, rdd, blen, -1, outputEmpty);
            }
        }

        MetaData md = new MetaData(mcBc);
        MatrixObject mo = new MatrixObject(Types.ValueType.FP64, "", md, bcast);
        PartitionedBroadcast<MatrixBlock> in2 = ExecutionUtil.broadcastForMatrixObject(sc, mo);

        if (!outputEmpty)
            in1 = in1.filter(new FilterNonEmptyBlocksFunction());

        if (_aggType == SparkAggTye.SINGLE_BLOCK) {
            JavaRDD<MatrixBlock> out = in1.map(new RDDMapMMFunction2(type, in2));
            return RDDAggregateUtils.sumStable(out);
        } else {
            JavaPairRDD<MatrixIndexes, MatrixBlock> out = null;
            if (requiresFlatMapFunction(type, mcBc)) {
                if (requiresRepartitioning(type, mcRdd, mcBc, in1.getNumPartitions())) {
                    int numPart = getNumRepartitioning(type, mcRdd, mcBc);
                    in1 = in1.repartition(numPart);
                }
                out = in1.flatMapToPair(new RDDFlatMapMMFunction(type, in2));
            } else if (preservesPartitioning(mcRdd, type))
                out = in1.mapPartitionsToPair(new RDDMapMMPartitionFunction(type, in2), true);
            else
                out = in1.mapToPair(new RDDMapMMFunction(type, in2));

            if (!outputEmpty)
                out = out.filter(new FilterNonEmptyBlocksFunction());

            if (_aggType == SparkAggTye.MULTI_BLOCK)
                out = RDDAggregateUtils.sumByKeyStable(out, false);

            return SparkExecutionContext.toMatrixBlock(out, a.getNumRows(), b.getNumColumns(), blen, -1);
        }

    }

    private static boolean preservesPartitioning(DataCharacteristics mcRdd, CacheTpye type) {
        if (type == CacheTpye.LEFT)
            return mcRdd.dimsKnown() && mcRdd.getRows() <= mcRdd.getBlocksize();
        else
            return mcRdd.dimsKnown() && mcRdd.getCols() <= mcRdd.getBlocksize();
    }

    private static boolean requiresRepartitioning(CacheTpye type, DataCharacteristics mcRdd, DataCharacteristics mcBc, int numPartitions) {
        boolean isLeft = (type == CacheTpye.LEFT);
        boolean isOuter = isLeft ?
                (mcRdd.getRows() <= mcRdd.getBlocksize()) :
                (mcRdd.getCols() <= mcRdd.getBlocksize());
        boolean isLargeOutput = (OptimizerUtils.estimatePartitionedSizeExactSparsity(isLeft ? mcBc.getRows() : mcRdd.getRows(),
                isLeft ? mcRdd.getCols() : mcBc.getCols(), mcRdd.getBlocksize(), 1.0) / numPartitions) > 1024 * 1024 * 1024;
        return isOuter && isLargeOutput && mcRdd.dimsKnown() && mcBc.dimsKnown()
                && numPartitions < getNumRepartitioning(type, mcRdd, mcBc);
    }

    private static boolean requiresFlatMapFunction(CacheTpye type, DataCharacteristics mcBc) {
        return (type == CacheTpye.LEFT && mcBc.getRows() > mcBc.getBlocksize())
                || (type == CacheTpye.RIGHT && mcBc.getCols() > mcBc.getBlocksize());
    }

    private static int getNumRepartitioning(CacheTpye type, DataCharacteristics mcRdd, DataCharacteristics mcBc) {
        boolean isLeft = (type == CacheTpye.LEFT);
        long sizeOutput = OptimizerUtils.estimatePartitionedSizeExactSparsity(isLeft ? mcBc.getRows() : mcRdd.getRows(),
                isLeft ? mcRdd.getCols() : mcBc.getCols(), mcRdd.getBlocksize(), 1.0);
        long numParts = sizeOutput / InfrastructureAnalyzer.getHDFSBlockSize();
        return (int) Math.min(numParts, (isLeft ? mcRdd.getNumColBlocks() : mcRdd.getNumRowBlocks()));
    }

    private static class RDDMapMMFunction2 implements Function<Tuple2<MatrixIndexes, MatrixBlock>, MatrixBlock> {

        private final CacheTpye _type;
        private final AggregateBinaryOperator _op;
        private final PartitionedBroadcast<MatrixBlock> _pbc;

        public RDDMapMMFunction2(CacheTpye type, PartitionedBroadcast<MatrixBlock> in2) {
            _type = type;
            _pbc = in2;
            AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
            _op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
        }

        @Override
        public MatrixBlock call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
            MatrixIndexes ixIn = arg0._1();
            MatrixBlock blkIn = arg0._2();

            if (_type == CacheTpye.LEFT) {
                MatrixBlock left = _pbc.getBlock(1, (int) ixIn.getRowIndex());
                return OperationsOnMatrixValues.matMult(left, blkIn, new MatrixBlock(), _op);
            } else {
                MatrixBlock right = _pbc.getBlock((int) ixIn.getColumnIndex(), 1);
                return OperationsOnMatrixValues.matMult(blkIn, right, new MatrixBlock(), _op);
            }
        }
    }

    private static class RDDFlatMapMMFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> {
        private final CacheTpye _type;
        private final AggregateBinaryOperator _op;
        private final PartitionedBroadcast<MatrixBlock> _pbc;

        public RDDFlatMapMMFunction(CacheTpye type, PartitionedBroadcast<MatrixBlock> in2) {
            _type = type;
            _pbc = in2;
            _op = InstructionUtils.getMatMultOperator(1);
        }

        @Override
        public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
                throws Exception {
            MatrixIndexes ixIn = arg0._1();
            MatrixBlock blkIn = arg0._2();
            if (_type == CacheTpye.LEFT) {
                return IntStream.range(1, _pbc.getNumRowBlocks() + 1).mapToObj(i ->
                        new Tuple2<>(new MatrixIndexes(i, ixIn.getColumnIndex()),
                                OperationsOnMatrixValues.matMult(_pbc.getBlock(i, (int) ixIn.getRowIndex()), blkIn,
                                        new MatrixBlock(), _op))).iterator();
            } else {
                return IntStream.range(1, _pbc.getNumColumnBlocks() + 1).mapToObj(j ->
                        new Tuple2<>(new MatrixIndexes(ixIn.getRowIndex(), j),
                                OperationsOnMatrixValues.matMult(blkIn, _pbc.getBlock((int) ixIn.getColumnIndex(), j),
                                        new MatrixBlock(), _op))).iterator();
            }
        }
    }

    private static class RDDMapMMPartitionFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes, MatrixBlock>>, MatrixIndexes, MatrixBlock> {
        private final CacheTpye _type;
        private final AggregateBinaryOperator _op;
        private final PartitionedBroadcast<MatrixBlock> _pbc;

        public RDDMapMMPartitionFunction(CacheTpye type, PartitionedBroadcast<MatrixBlock> in2) {
            _type = type;
            _pbc = in2;
            AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
            _op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
        }

        @Override
        public LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> arg0) throws Exception {
            return new MamMMPartitionIterator(arg0);
        }

        private class MamMMPartitionIterator extends LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> {
            public MamMMPartitionIterator(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> arg0) {
                super(arg0);
            }

            @Override
            protected Tuple2<MatrixIndexes, MatrixBlock> computeNext(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
                MatrixIndexes ixIn = arg0._1();
                MatrixBlock blkIn = arg0._2();
                MatrixBlock blkOut;

                if (_type == CacheTpye.LEFT) {
                    MatrixBlock left = _pbc.getBlock(1, (int) ixIn.getRowIndex());
                    blkOut = OperationsOnMatrixValues.matMult(left, blkIn, _op);
                } else {
                    MatrixBlock right = _pbc.getBlock((int) ixIn.getColumnIndex(), 1);
                    blkOut = OperationsOnMatrixValues.matMult(blkIn, right, _op);
                }
                return new Tuple2<>(ixIn, blkOut);
            }
        }
    }

    private static class RDDMapMMFunction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> {
        private final CacheTpye _type;
        private final AggregateBinaryOperator _op;
        private final PartitionedBroadcast<MatrixBlock> _pbc;

        public RDDMapMMFunction(CacheTpye type, PartitionedBroadcast<MatrixBlock> in2) {
            _type = type;
            _pbc = in2;
            _op = InstructionUtils.getMatMultOperator(1);
        }

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
            MatrixIndexes ixIn = arg0._1();
            MatrixBlock blkIn = arg0._2();

            MatrixIndexes ixOut = new MatrixIndexes();
            MatrixBlock blkOut = new MatrixBlock();

            if (_type == CacheTpye.LEFT) {
                MatrixBlock left = _pbc.getBlock(1, (int) ixIn.getRowIndex());
                OperationsOnMatrixValues.matMult(new MatrixIndexes(1, ixIn.getRowIndex()),
                        left, ixIn, blkIn, ixOut, blkOut, _op);
            } else {
                MatrixBlock right = _pbc.getBlock((int) ixIn.getColumnIndex(), 1);
                OperationsOnMatrixValues.matMult(ixIn, blkIn, new MatrixIndexes(ixIn.getColumnIndex(), 1),
                        right, ixOut, blkOut, _op);
            }
            return new Tuple2<>(ixOut, blkOut);
        }
    }
}
