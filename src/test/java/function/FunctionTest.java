package function;

import com.distributedMacPlayground.config.CommonConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.instructions.spark.utils.RDDConverterUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.junit.Assert;
import org.junit.Test;

public class FunctionTest {
    //
//    @Test
//    public void getMatrixByCSVFile() {
//        System.setProperty("hadoop.home.dir", "D:\\hadoop");
//        SparkConf sparkConf = new SparkConf().setAppName("testFunction").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        sc.setLogLevel("ERROR");
//        JavaRDD<String> csvData = sc.textFile(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/in1.csv");
//        MatrixCharacteristics mc = new MatrixCharacteristics(100, 200, 10);
//        JavaPairRDD<MatrixIndexes, MatrixBlock> rdd = RDDConverterUtils.csvToBinaryBlock(sc, csvData, mc, false, ",", true, 0, null);
//        JavaRDD<String> tmp = RDDConverterUtils.binaryBlockToCsv(rdd, mc, new FileFormatPropertiesCSV(), true);
////        tmp.saveAsTextFile(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/in1");
//        sc.close();
//        System.out.println();
//    }
//
//    @Test
//    public void generateLargeMatrix() throws Exception {
//        String indexFileName = CommonConfig.SYNTHETICDATASET_DIR + "100x300x100/100x300x10_matrix_index.csv";
//        SparkConf sparkConf = new SparkConf().setAppName("testFunction").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        sc.setLogLevel("ERROR");
//        RandomMatrixRDDGenerator generator = new RandomMatrixRDDGenerator(2, 5, 1.0, 1023);
//        JavaPairRDD<MatrixIndexes, MatrixBlock> out = generator.generate(sc, indexFileName);
//        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, generator.getRlen(), generator.getClen(), generator.getBlockSize(), -1);
//        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Function.FunctionTest/generate.csv", res);
//        System.out.println("Calculate successfully! You can find this output from ./src/test/cache/Function.FunctionTest");
//    }
    @Test
    public void IOTest() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("IO").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        String path = CommonConfig.OUTPUT_BUFFER_DIR + "IO/in1.csv";
        MatrixCharacteristics mc = new MatrixCharacteristics(20, 20, 10);
        JavaPairRDD<MatrixIndexes, MatrixBlock> tmp = RDDConverterUtils.csvToBinaryBlock(sc, sc.textFile(path), mc, false, ",", true, 0, null);
        MatrixBlock matrixBlock = SparkExecutionContext.toMatrixBlock(tmp, (int) mc.getRows(), (int) mc.getCols(), mc.getBlocksize(), -1);
        Assert.assertNull(matrixBlock.getDenseBlock());
        Assert.assertNotNull(matrixBlock.getSparseBlock());
        sc.close();
    }

    @Test
    public void anyTest() throws Exception{

    }
}
