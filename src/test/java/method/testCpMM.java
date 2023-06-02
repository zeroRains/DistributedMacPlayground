package method;

import com.distributedMacPlayground.config.CommonConfig;
import com.distributedMacPlayground.method.CpMM;
import com.distributedMacPlayground.method.MatrixMultiply;
import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.junit.Test;

public class testCpMM {
    @Test
    public void testCpMM() throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("testCpmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(100, 200, 200, 200, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/in2.csv", data.in2Block);
//        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/in3.csv", data.in3Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
//        JavaPairRDD<MatrixIndexes, MatrixBlock> in3 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in3Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        MatrixMultiply mm = new CpMM();

//        （in1 x in2) x in3
        JavaPairRDD<MatrixIndexes, MatrixBlock> out1 = mm.execute(in1, in2, data.mc1, data.mc2);
        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out1, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
//        MatrixCharacteristics outMc = new MatrixCharacteristics(data.in1Block.getNumRows(), data.in2Block.getNumColumns(), 10, data.in1Block.getNonZeros());
//        JavaPairRDD<MatrixIndexes, MatrixBlock> out = mm.execute(out1, in3, outMc, data.mc3);

//        in1 x (in2 x in3)
//        JavaPairRDD<MatrixIndexes, MatrixBlock> out1 = Cpmm.execute(in2, in3, data.mc2, data.mc3);
//        MatrixCharacteristics outMc = new MatrixCharacteristics(data.in2Block.getNumRows(), data.in3Block.getNumColumns(), 10, data.in2Block.getNonZeros());
//        JavaPairRDD<MatrixIndexes, MatrixBlock> out = Cpmm.execute(in1, out1, data.mc1, outMc);

//        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc3.getCols(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Cpmm");
        sc.close();
    }


}
