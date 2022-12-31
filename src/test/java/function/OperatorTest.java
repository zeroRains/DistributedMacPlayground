package function;

import com.distributedMacPlayground.config.CommonConfig;
import com.distributedMacPlayground.operator.Operator;
import com.distributedMacPlayground.util.IOUtil;
import method.SimpleMatrixMulData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.junit.Test;

public class OperatorTest {
    @Test
    public void transpose() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testTranspose").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        int blen = 1000;

        SimpleMatrixMulData data = new SimpleMatrixMulData(4000, 3000, 2, 1, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Operator/in1.csv", data.in1Block);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false);
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = Operator.transpose(in1);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getCols(), (int) data.mc1.getRows(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Operator/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Operator");
    }

    @Test
    public void elementWiseMultiply() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testTranspose").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        int blen = 2;

        SimpleMatrixMulData data = new SimpleMatrixMulData(4, 4, 4, 4, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Operator/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Operator/in2.csv", data.in2Block);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false);
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = Operator.elementWiseProduct(in1, in2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getCols(), (int) data.mc1.getRows(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Operator/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Operator");
    }

    @Test
    public void elementWiseDivision() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testTranspose").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        int blen = 2;

        SimpleMatrixMulData data = new SimpleMatrixMulData(4, 4, 4, 4, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Operator/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Operator/in2.csv", data.in2Block);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false);
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = Operator.elementWiseDivision(in1, in2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getCols(), (int) data.mc1.getRows(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Operator/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Operator");
    }
}
