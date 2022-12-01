import com.distributedMacPlayground.CommonConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.sysds.api.mlcontext.MLContextConversionUtil;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.instructions.spark.utils.RDDConverterUtils;
import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;

public class FunctionTest {

    @Test
    public void getMatrixByCSVFile() {
        System.setProperty("hadoop.home.dir","D:\\hadoop");
        SparkConf sparkConf = new SparkConf().setAppName("testMapmm").setMaster("local");
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
}
