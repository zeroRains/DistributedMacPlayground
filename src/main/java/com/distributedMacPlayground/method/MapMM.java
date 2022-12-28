package com.distributedMacPlayground.method;

import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.CommonConfig.SparkAggType;
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
import org.apache.sysds.runtime.meta.MetaData;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Iterator;
import java.util.stream.IntStream;

public class MapMM implements MatrixMultiply{
    private int blen = 1000;
    private CacheTpye type = CacheTpye.LEFT;
    private SparkAggType _aggType = SparkAggType.MULTI_BLOCK;
    private JavaSparkContext sc = null;
    private boolean outputEmpty = true;

    public void setBlen(int blen) {
        this.blen = blen;
    }

    public void setType(CacheTpye type) {
        this.type = type;
    }

    public void set_aggType(SparkAggType _aggType) {
        this._aggType = _aggType;
    }

    public void setSc(JavaSparkContext sc) {
        this.sc = sc;
    }

    public void setOutputEmpty(boolean outputEmpty) {
        this.outputEmpty = outputEmpty;
    }

    // format a x b
    @Override
    public JavaPairRDD<MatrixIndexes, MatrixBlock> execute(JavaPairRDD<MatrixIndexes, MatrixBlock> a,
                                                           JavaPairRDD<MatrixIndexes, MatrixBlock> b, DataCharacteristics mc1,
                                                           DataCharacteristics mc2) throws Exception {
        JavaPairRDD<MatrixIndexes, MatrixBlock> rdd = type.isRight() ? a : b;
        JavaPairRDD<MatrixIndexes, MatrixBlock> bcast = type.isRight() ? b : a;
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1;
        DataCharacteristics mcRdd = type.isRight() ? mc1 : mc2;
        DataCharacteristics mcBc = type.isRight() ? mc2 : mc1;

        int numParts = (requiresFlatMapFunction(type, mcBc) && requiresRepartitioning(type, mcRdd, mcBc,
                sc.defaultMinPartitions())) ? getNumRepartitioning(type, mcRdd, mcBc) : -1;
        if (numParts > 1)
            in1 = rdd.repartition(numParts);
        else
            in1 = rdd;


        if (requiresFlatMapFunction(type, mcBc) &&
                requiresRepartitioning(type, mcRdd, mcBc, in1.getNumPartitions())) {
            int numParts1 = getNumRepartitioning(type, mcRdd, mcBc);
            int numParts2 = getNumRepartitioning(type.getFilped(), mcBc, mcRdd);
            if (numParts2 > numParts1) {
                type = type.getFilped();
                rdd = type.isRight() ? a : b;
                bcast = type.isRight() ? b : a;
                mcRdd = type.isRight() ? mc1 : mc2;
                mcBc = type.isRight() ? mc2 : mc1;
                in1 = rdd;
            }
        }

        MetaData md = new MetaData(mcBc);
        MatrixBlock blkBroadcast = SparkExecutionContext.toMatrixBlock(bcast, (int) mcBc.getRows(), (int) mcBc.getCols(), blen, -1);
        MatrixObject mo = new MatrixObject(Types.ValueType.FP64, "", md, blkBroadcast);
        PartitionedBroadcast<MatrixBlock> in2 = ExecutionUtil.broadcastForMatrixObject(sc, mo);

        if (!outputEmpty)
            in1 = in1.filter(new FilterNonEmptyBlocksFunction());

        if (_aggType == SparkAggType.SINGLE_BLOCK) {
            JavaPairRDD<MatrixIndexes, MatrixBlock> out = in1.mapToPair(new RDDMapMMFunction2(type, in2));
            return RDDAggregateUtils.sumByKeyStable(out);
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

            if (_aggType == SparkAggType.MULTI_BLOCK)
                out = RDDAggregateUtils.sumByKeyStable(out, false);

            return out;
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

    private static class RDDMapMMFunction2 implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> {
        private final CacheTpye _type;
        private final AggregateBinaryOperator _op;
        private final PartitionedBroadcast<MatrixBlock> _pbc;

        public RDDMapMMFunction2(CacheTpye type, PartitionedBroadcast<MatrixBlock> in2) {
            _type = type;
            _pbc = in2;
            _op = InstructionUtils.getMatMultOperator(1);
        }

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
            MatrixIndexes ixIn = arg0._1();
            MatrixBlock blkIn = arg0._2();

            if (_type == CacheTpye.LEFT) {
                MatrixBlock left = _pbc.getBlock(1, (int) ixIn.getRowIndex());
                return new Tuple2<>(new MatrixIndexes(1, 1),
                        OperationsOnMatrixValues.matMult(left, blkIn, new MatrixBlock(), _op));
            } else {
                MatrixBlock right = _pbc.getBlock((int) ixIn.getColumnIndex(), 1);
                return new Tuple2<>(new MatrixIndexes(1, 1),
                        OperationsOnMatrixValues.matMult(blkIn, right, new MatrixBlock(), _op));
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
