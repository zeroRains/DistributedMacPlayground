package method;

import com.distributedMacPlayground.config.CommonConfig;
import com.distributedMacPlayground.method.MapMM;
import com.distributedMacPlayground.method.MatrixMultiply;
import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.junit.Test;

public class testMapMM {
    MatrixMultiply mm;

    @Test
    public void testLeftBroadcastMultiBlockFlatMap() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        int blen = 10;
        CommonConfig.CacheTpye type = CommonConfig.CacheTpye.LEFT;
        CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;
        boolean outputEmpty = false;

        SimpleMatrixMulData data = new SimpleMatrixMulData(100, 3000, 3000, 200, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式

        MapMM mapMM = new MapMM();
        mapMM.setBlen(blen);
        mapMM.setSc(sc);
        mapMM.setType(type);
        mapMM.set_aggType(_aggType);
        mapMM.setOutputEmpty(outputEmpty);
        mm = mapMM;
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = mm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
        sc.close();
    }

    @Test
    public void testLeftBroadcastMultiBlockPreservesPartitioning() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        int blen = 10;
        CommonConfig.CacheTpye type = CommonConfig.CacheTpye.LEFT;
        CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;
        boolean outputEmpty = false;

        SimpleMatrixMulData data = new SimpleMatrixMulData(10, 10, 10, 300, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        MapMM mapMM = new MapMM();
        mapMM.setBlen(blen);
        mapMM.setSc(sc);
        mapMM.setType(type);
        mapMM.set_aggType(_aggType);
        mapMM.setOutputEmpty(outputEmpty);
        mm = mapMM;
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = mm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
        sc.close();
    }

    @Test
    public void testLeftBroadcastMultiBlockDefault() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        int blen = 10;
        CommonConfig.CacheTpye type = CommonConfig.CacheTpye.LEFT;
        CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;
        boolean outputEmpty = false;

        SimpleMatrixMulData data = new SimpleMatrixMulData(10, 100, 100, 300, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式

        MapMM mapMM = new MapMM();
        mapMM.setBlen(blen);
        mapMM.setSc(sc);
        mapMM.setType(type);
        mapMM.set_aggType(_aggType);
        mapMM.setOutputEmpty(outputEmpty);
        mm = mapMM;
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = mm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
        sc.close();
    }

    @Test
    public void testLeftBroadcastSingleBlockDefault() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        int blen = 100;
        CommonConfig.CacheTpye type = CommonConfig.CacheTpye.LEFT;
        CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.SINGLE_BLOCK;
        boolean outputEmpty = false;

        SimpleMatrixMulData data = new SimpleMatrixMulData(50, 70, 70, 70, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式

        MapMM mapMM = new MapMM();
        mapMM.setBlen(blen);
        mapMM.setSc(sc);
        mapMM.setType(type);
        mapMM.set_aggType(_aggType);
        mapMM.setOutputEmpty(outputEmpty);
        mm = mapMM;
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = mm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
        sc.close();
    }

    @Test
    public void testRightBroadcastSingleBlockDefault() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        int blen = 100;
        CommonConfig.CacheTpye type = CommonConfig.CacheTpye.RIGHT;
        CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.SINGLE_BLOCK;
        boolean outputEmpty = false;

        SimpleMatrixMulData data = new SimpleMatrixMulData(50, 70, 70, 70, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式

        MapMM mapMM = new MapMM();
        mapMM.setBlen(blen);
        mapMM.setSc(sc);
        mapMM.setType(type);
        mapMM.set_aggType(_aggType);
        mapMM.setOutputEmpty(outputEmpty);
        mm = mapMM;
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = mm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
        sc.close();
    }

    @Test
    public void testRightBroadcastMultiBlockFlatMap() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        int blen = 1;
        CommonConfig.CacheTpye type = CommonConfig.CacheTpye.RIGHT;
        CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;
        boolean outputEmpty = false;

        SimpleMatrixMulData data = new SimpleMatrixMulData(300, 200, 200, 100, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        MapMM mapMM = new MapMM();
        mapMM.setBlen(blen);
        mapMM.setSc(sc);
        mapMM.setType(type);
        mapMM.set_aggType(_aggType);
        mapMM.setOutputEmpty(outputEmpty);
        mm = mapMM;
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = mm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
        sc.close();
    }

    @Test
    public void testRightBroadcastMultiBlockPreservesPartitioning() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        int blen = 10;
        CommonConfig.CacheTpye type = CommonConfig.CacheTpye.RIGHT;
        CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;
        boolean outputEmpty = false;

        SimpleMatrixMulData data = new SimpleMatrixMulData(300, 10, 10, 10, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        MapMM mapMM = new MapMM();
        mapMM.setBlen(blen);
        mapMM.setSc(sc);
        mapMM.setType(type);
        mapMM.set_aggType(_aggType);
        mapMM.setOutputEmpty(outputEmpty);
        mm = mapMM;
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = mm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
        sc.close();
    }

    @Test
    public void testRightBroadcastMultiBlockDefault() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        int blen = 10;
        CommonConfig.CacheTpye type = CommonConfig.CacheTpye.RIGHT;
        CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;
        boolean outputEmpty = false;

        SimpleMatrixMulData data = new SimpleMatrixMulData(300, 100, 100, 10, 1, 1, 2, 2, 5, 5, "uniform", 1023, blen);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        MapMM mapMM = new MapMM();
        mapMM.setBlen(blen);
        mapMM.setSc(sc);
        mapMM.setType(type);
        mapMM.set_aggType(_aggType);
        mapMM.setOutputEmpty(outputEmpty);
        mm = mapMM;
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = mm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
        sc.close();
    }

    @Test
    public void testLeftBroadcastMultiBlockDefaultSparse() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        int blen = 10;
        CommonConfig.CacheTpye type = CommonConfig.CacheTpye.LEFT;
        CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;
        boolean outputEmpty = false;

        SimpleMatrixMulData data = new SimpleMatrixMulData(10, 100, 100, 300, 0.1, 0.1, 2, 2, 5, 5, "uniform", 1023, blen);

        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in1.csv", data.in1Block);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/in2.csv", data.in2Block);
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, blen, -1, false); // 将MatrixBlock转化成RDD的方式
        MapMM mapMM = new MapMM();
        mapMM.setBlen(blen);
        mapMM.setSc(sc);
        mapMM.setType(type);
        mapMM.set_aggType(_aggType);
        mapMM.setOutputEmpty(outputEmpty);
        mm = mapMM;
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = mm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);

        IOUtil.outputMatrixToLocalCSV(System.getProperty("user.dir") + "/" + CommonConfig.OUTPUT_BUFFER_DIR + "Mapmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Mapmm");
        sc.close();
    }

}
