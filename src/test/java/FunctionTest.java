import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.util.IOUtil;
import com.distributedMacPlayground.util.RandomMatrixRDDGenerator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.instructions.spark.utils.RDDConverterUtils;
import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.junit.Test;

public class FunctionTest {

    @Test
    public void getMatrixByCSVFile() {
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf sparkConf = new SparkConf().setAppName("testFunction").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        JavaRDD<String> csvData = sc.textFile(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/in1.csv");
        MatrixCharacteristics mc = new MatrixCharacteristics(100, 200, 10);
        JavaPairRDD<MatrixIndexes, MatrixBlock> rdd = RDDConverterUtils.csvToBinaryBlock(sc, csvData, mc, false, ",", true, 0, null);
        JavaRDD<String> tmp = RDDConverterUtils.binaryBlockToCsv(rdd, mc, new FileFormatPropertiesCSV(), true);
//        tmp.saveAsTextFile(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/in1");
        sc.close();
        System.out.println();
    }

    @Test
    public void generateLargeMatrix() throws Exception {
        // TODO: need to make sure that the index files are stored in CommonConfig.SYNTHETICDATASET_DIR
        String indexFileName = CommonConfig.SYNTHETICDATASET_DIR + "100x300x100/100x300x10_matrix_index.csv";
        SparkConf sparkConf = new SparkConf().setAppName("testFunction").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        RandomMatrixRDDGenerator generator = new RandomMatrixRDDGenerator(2, 5, 1.0, 1023);
        JavaPairRDD<MatrixIndexes, MatrixBlock> out = generator.generate(sc, indexFileName);
        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, generator.getRlen(), generator.getClen(), generator.getBlockSize(), -1);
        IOUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "FunctionTest/generate.csv", res);
        System.out.println("Calculate successfully! You can find this output from ./src/test/cache/FunctionTest");
    }

    @Test
    public void testForTest() {
        System.out.println("-in1".toUpperCase());
    }


}
