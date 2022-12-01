package method;

import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.method.MapMM;
import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.junit.Test;

public class testMapMM {
    @Test
    public void testLeftBroadcastMultiBlockFlatMap() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(100, 200, 200, 300, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        MatrixBlock res = MapMM.execute(sc, data.in1Block, data.in2Block, 10, CommonConfig.CacheTpye.LEFT, CommonConfig.SparkAggType.MULTI_BLOCK, false);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
    }

    @Test
    public void testLeftBroadcastMultiBlockPreservesPartitioning() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(10, 10, 10, 300, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        MatrixBlock res = MapMM.execute(sc, data.in1Block, data.in2Block, 10, CommonConfig.CacheTpye.LEFT, CommonConfig.SparkAggType.MULTI_BLOCK, false);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
    }

    @Test
    public void testLeftBroadcastMultiBlockDefault() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(10, 100, 100, 300, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        MatrixBlock res = MapMM.execute(sc, data.in1Block, data.in2Block, 10, CommonConfig.CacheTpye.LEFT, CommonConfig.SparkAggType.MULTI_BLOCK, false);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
    }

    @Test
    public void testLeftBroadcastSingleBlockDefault() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(50, 70, 70, 70, 1, 1, 2, 2, 5, 5, "uniform", 1023, 100);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        MatrixBlock res = MapMM.execute(sc, data.in1Block, data.in2Block, 100, CommonConfig.CacheTpye.LEFT, CommonConfig.SparkAggType.SINGLE_BLOCK, false);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
    }

    @Test
    public void testRightBroadcastSingleBlockDefault() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(50, 70, 70, 70, 1, 1, 2, 2, 5, 5, "uniform", 1023, 100);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        MatrixBlock res = MapMM.execute(sc, data.in1Block, data.in2Block, 100, CommonConfig.CacheTpye.RIGHT, CommonConfig.SparkAggType.SINGLE_BLOCK, false);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
    }

    @Test
    public void testRightBroadcastMultiBlockFlatMap() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(300, 200, 200, 100, 1, 1, 2, 2, 5, 5, "uniform", 1023, 1);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        MatrixBlock res = MapMM.execute(sc, data.in1Block, data.in2Block, 10, CommonConfig.CacheTpye.RIGHT, CommonConfig.SparkAggType.MULTI_BLOCK, false);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
    }

    @Test
    public void testRightBroadcastMultiBlockPreservesPartitioning() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(300, 10, 10, 10, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        MatrixBlock res = MapMM.execute(sc, data.in1Block, data.in2Block, 10, CommonConfig.CacheTpye.RIGHT, CommonConfig.SparkAggType.MULTI_BLOCK, false);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
    }

    @Test
    public void testRightBroadcastMultiBlockDefault() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(300, 100, 100, 10, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        MatrixBlock res = MapMM.execute(sc, data.in1Block, data.in2Block, 10, CommonConfig.CacheTpye.RIGHT, CommonConfig.SparkAggType.MULTI_BLOCK, false);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
    }

    @Test
    public void testLeftBroadcastMultiBlockDefaultSparse() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(10, 100, 100, 300, 0.1, 0.1, 2, 2, 5, 5, "uniform", 1023, 10);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        MatrixBlock res = MapMM.execute(sc, data.in1Block, data.in2Block, 10, CommonConfig.CacheTpye.LEFT, CommonConfig.SparkAggType.MULTI_BLOCK, false);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
    }
}
