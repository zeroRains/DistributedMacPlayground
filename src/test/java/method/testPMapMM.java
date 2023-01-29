package method;

import com.distributedMacPlayground.config.CommonConfig;
import com.distributedMacPlayground.method.PMapMM;
import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.junit.Test;

public class testPMapMM {
    @Test
    public void testPMapMMDenseMatrix() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(200, 300, 300, 100, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/in2.csv", data.in2Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = PMapMM.execute(sc, in1, in2, data.mc1);
        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/PMapmm");
        sc.close();
    }

    @Test
    public void testPMapMMSparseMatrix() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(200, 300, 300, 100, 0.1, 0.1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/in2.csv", data.in2Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = PMapMM.execute(sc, in1, in2, data.mc1);
        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/PMapmm");
        sc.close();
    }

    // from this test, we should make sure broadcast matrix must be dense. Because sparse vector will make broadcast cache block be null.
    @Test
    public void testPMapMMDenseVectorMatrix() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(1, 300, 300, 100, 1, 0.1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/in2.csv", data.in2Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = PMapMM.execute(sc, in1, in2, data.mc1);
        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/PMapmm");
        sc.close();
    }

    @Test
    public void testPMapMMMatrixVector() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(100, 300, 300, 1, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/in2.csv", data.in2Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = PMapMM.execute(sc, in1, in2, data.mc1);
        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/PMapmm");
        sc.close();
    }

    @Test
    public void testPMapMMVectorVector() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(1, 300, 300, 1, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/in2.csv", data.in2Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = PMapMM.execute(sc, in1, in2, data.mc1);
        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "PMapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/PMapmm");
        sc.close();
    }
}
