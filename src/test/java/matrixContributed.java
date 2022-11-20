import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;

import java.util.ArrayList;

public class matrixContributed {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        MatrixBlock res = MatrixBlock.randOperations(10000, 10000, 0.3, 2, 11, "uniform", 1023);
        JavaPairRDD<MatrixIndexes, MatrixBlock> data = SparkExecutionContext.toMatrixJavaPairRDD(sc, res, 10); // 将MatrixBlock转化成RDD的方式
        System.out.println("233");
    }
}
