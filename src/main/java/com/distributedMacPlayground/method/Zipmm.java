package com.distributedMacPlayground.method;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.functionobjects.SwapIndex;
import org.apache.sysds.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysds.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import scala.Tuple2;

public class Zipmm {
    // use in the type like t(B)xa-> t(t(a)xB)
    // t() denotes transpose, a is a vector and B is a matrix
    public static MatrixBlock execute(JavaPairRDD<MatrixIndexes, MatrixBlock> in1,
                                                                  JavaPairRDD<MatrixIndexes, MatrixBlock> in2,
                                                                  DataCharacteristics mc1, DataCharacteristics mc2, boolean _tRewrite) throws Exception {
        JavaRDD<MatrixBlock> out = in1.join(in2).values().map(new ZipMultiplyFunction(_tRewrite));
        MatrixBlock out2 = RDDAggregateUtils.sumStable(out);

        if(_tRewrite){
            ReorgOperator rop = new ReorgOperator(SwapIndex.getSwapIndexFnObject());
            out2 = out2.reorgOperations(rop,new MatrixBlock(),0,0,0);
        }

        return out2;
    }

    private static class ZipMultiplyFunction implements Function<Tuple2<MatrixBlock, MatrixBlock>, MatrixBlock> {

        private AggregateBinaryOperator _abop = null;
        private ReorgOperator _rop = null;
        private boolean _tRewrite = true;

        public ZipMultiplyFunction(boolean tRewrite) {
            _tRewrite = tRewrite;
            AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
            _abop = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
            _rop = new ReorgOperator(SwapIndex.getSwapIndexFnObject());
        }

        @Override
        public MatrixBlock call(Tuple2<MatrixBlock, MatrixBlock> arg0) throws Exception {
            MatrixBlock in1 = _tRewrite ? arg0._1() : arg0._2();
            MatrixBlock in2 = _tRewrite ? arg0._2() : arg0._1();
            MatrixBlock tmp = in2.reorgOperations(_rop, new MatrixBlock(), 0, 0, 0);
            return OperationsOnMatrixValues.matMult(tmp, in1, new MatrixBlock(), _abop);
        }
    }
}

