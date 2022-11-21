package method;

import com.distributedMacPlayground.util.ExecutionUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaData;

public class testPmm {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("testPmm").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        MatrixBlock block = MatrixBlock.randOperations(4, 4, 1, 1, 2, "uniform", 2096);
        MatrixCharacteristics mc = new MatrixCharacteristics(block.getNumRows(), block.getNumColumns(), 10, block.getNonZeros());
        MetaData md = new MetaData(mc);
        MatrixObject mo = new MatrixObject(Types.ValueType.FP64, "", md, block);
        PartitionedBroadcast<MatrixBlock> res = ExecutionUtil.broadcastForMatrixObject(sc, mo);
        System.out.println("233");
    }
}
