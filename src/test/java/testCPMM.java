import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.method.CPMM;
import com.distributedMacPlayground.util.OutputUtil;
import org.apache.jasper.tagplugins.jstl.core.Out;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sysds.api.mlcontext.MLContextConversionUtil;
import org.apache.sysds.api.mlcontext.MatrixMetadata;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.instructions.spark.functions.FilterNonEmptyBlocksFunction;
import org.apache.sysds.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysds.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import scala.Tuple2;

public class testCPMM {

    public static void main(String[] args) throws Exception {
        // 构建Spark环境
        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        // 创建输入的矩阵块
        MatrixBlock in1Block = MatrixBlock.randOperations(300, 20, 1, 0, 1, "uniform", 1023);
        MatrixBlock in2Block = MatrixBlock.randOperations(20, 200, 1, 0, 1, "uniform", 1023);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "CPMM/in1.csv", in1Block);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "CPMM/in2.csv", in2Block);
        // 将输入转化成分布式矩阵的存储格式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, in1Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, in2Block, 10, -1, false); // 将MatrixBlock转化成RDD的方式
        // 获取矩阵块的数据特征
        DataCharacteristics mc1 = new MatrixCharacteristics(in1Block.getNumRows(), in1Block.getNumColumns(), 10, in1Block.getNonZeros());
        DataCharacteristics mc2 = new MatrixCharacteristics(in2Block.getNumRows(), in2Block.getNumColumns(), 10, in2Block.getNonZeros());

        JavaPairRDD<MatrixIndexes, MatrixBlock> out = CPMM.execute(in1, in2, mc1, mc2);

        MatrixBlock res = SparkExecutionContext.toMatrixBlock(out, (int) mc1.getRows(),
                (int) mc2.getCols(), mc1.getBlocksize(), -1);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "CPMM/out.csv", res);
        System.out.println("Calculate successfully! You can find this test input and output from ./src/test/cache/CPMM");
    }


}
