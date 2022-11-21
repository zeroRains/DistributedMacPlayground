package method;

import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.method.Cpmm;
import com.distributedMacPlayground.util.OutputUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;

public class testCpmm {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("testCpmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(100, 200, 200, 1, 1, 1, 2, 2, 5, 5, "uniform", 1023, 10);

        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/in1.csv", data.in1Block);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/in2.csv", data.in2Block);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = Cpmm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Cpmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Cpmm");
    }


}
