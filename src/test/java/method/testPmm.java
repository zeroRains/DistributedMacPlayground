package method;

import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.method.Pmm;
import com.distributedMacPlayground.util.ExecutionUtil;
import com.distributedMacPlayground.util.OutputUtil;
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

public class testPmm {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("testPmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(100, 100, 100, 2000, 1, 1, 1, 1, 2, 2, "uniform", 2096, 10);
        System.out.println("A: "+data.in1Block.getNumRows() + "x" + data.in1Block.getNumColumns());
        System.out.println("B: "+data.in2Block.getNumRows() + "x" + data.in2Block.getNumColumns());
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Pmm/in1.csv", data.in1Block);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Pmm/in2.csv", data.in2Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 10, -1, false);
        MatrixCharacteristics mc = new MatrixCharacteristics(data.in2Block.getNumRows(), data.in2Block.getNumColumns(), 10, data.in2Block.getNonZeros());
        MetaData md = new MetaData(mc);
        MatrixObject mo = new MatrixObject(ValueType.FP64, "./br.csv", md, data.in2Block);
        PartitionedBroadcast<MatrixBlock> broadData = ExecutionUtil.broadcastForMatrixObject(sc, mo);

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = Pmm.execute(in1, broadData, data.mc1, data.mc1.getRows());
        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
        System.out.println("res: "+res.getNumRows() + "x" + res.getNumColumns());
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Pmm/out.csv", res);
    }
}
