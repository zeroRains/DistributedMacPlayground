package method;

import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.method.CRMM;
import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.junit.Test;

public class testCRMM {
    @Test
    public void testSimpleCRMM() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testCRMM").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(100, 200, 200, 300, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "CRMM/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "CRMM/in2.csv", data.in2Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = CRMM.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "CRMM/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/CRMM");
    }
}
