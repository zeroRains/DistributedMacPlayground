package method;

import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.method.Rmm;
import com.distributedMacPlayground.util.OutputUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;

public class testRmm {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("testRmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SimpleMatrixMulData data = new SimpleMatrixMulData(300, 100, 100, 400, 0.1, 1, 2, 2, 5, 5, "uniform", 1023, 10);
        // 创建输入的矩阵块
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Rmm/in1.csv", data.in1Block);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Rmm/in2.csv", data.in2Block);
        // 将输入转化成分布式矩阵的存储格式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in1Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, data.in2Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = Rmm.execute(in1, in2, data.mc1, data.mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) data.mc1.getRows(), (int) data.mc2.getCols(), data.mc1.getBlocksize(), -1);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Rmm/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/Rmm");

    }
}
