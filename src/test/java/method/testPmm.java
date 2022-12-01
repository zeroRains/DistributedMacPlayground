package method;

import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.method.PMM;
import com.distributedMacPlayground.util.ExecutionUtil;
import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaData;

public class testPmm { // Pmm（permutation matrix multiplication on row）, I found only adjacent rows can permutation
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("testPmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(8, 8, 8, 1, 1, 1, 1, 0, 2, 1, "uniform", 2096, 3);
        data.in2Block.quickSetValue(0,0,2);
        data.in2Block.quickSetValue(1,0,1);
        data.in2Block.quickSetValue(2,0,4);
        data.in2Block.quickSetValue(3,0,3);
        data.in2Block.quickSetValue(4,0,6);
        data.in2Block.quickSetValue(5,0,5);
        data.in2Block.quickSetValue(6,0,8);
        data.in2Block.quickSetValue(7,0,7);

        System.out.println("A: "+data.in1Block.getNumRows() + "x" + data.in1Block.getNumColumns());
        System.out.println("B: "+data.in2Block.getNumRows() + "x" + data.in2Block.getNumColumns());

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Pmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Pmm/in2.csv", data.in2Block);

        // row permutation
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 3, -1, false);
        MatrixCharacteristics mc = new MatrixCharacteristics(data.in2Block.getNumRows(), data.in2Block.getNumColumns(), 3, data.in2Block.getNonZeros());
        MetaData md = new MetaData(mc);
        MatrixObject mo = new MatrixObject(ValueType.FP64, "", md, data.in2Block);
        PartitionedBroadcast<MatrixBlock> broadData = ExecutionUtil.broadcastForMatrixObject(sc, mo);

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = PMM.execute(in1, broadData, data.mc1, data.mc1.getRows());
        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc1.getCols(), data.mc1.getBlocksize(), -1);

        System.out.println("res: "+res.getNumRows() + "x" + res.getNumColumns());
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Pmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Pmm");
    }
}
