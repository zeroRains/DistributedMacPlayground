package method;

import com.distributedMacPlayground.CommonConfig;
import com.distributedMacPlayground.util.ExecutionUtil;
import com.distributedMacPlayground.util.OutputUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.instructions.cp.ScalarObjectFactory;
import org.apache.sysds.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaData;
import org.dmg.pmml.Matrix;

public class testPmm {
    public static void main(String[] args) throws Exception {
        MatrixBlock c = new MatrixBlock();
        MatrixBlock d = new MatrixBlock();
        MatrixBlock a = MatrixBlock.randOperations(2, 2, 1, 1, 2, "uniform", 2039);
        MatrixBlock b = MatrixBlock.randOperations(2, 2, 1, 1, 2, "uniform", 2039);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Pmm/in1.csv", a);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Pmm/in1.csv", b);
        c.reset(2, a.getNumColumns(), false);
        d.reset(2, a.getNumColumns(), false);
        a.permutationMatrixMultOperations(b, c, null);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Pmm/in3.csv", c);
        OutputUtil.outputMatrixToLocalCSV(CommonConfig.OUTPUT_BUFFER_DIR + "Pmm/in4.csv", d);
        System.out.println("finished");
//        SparkConf conf = new SparkConf().setAppName("testPmm").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        MatrixBlock block = MatrixBlock.randOperations(4, 4, 1, 1, 2, "uniform", 2096);
//        MatrixCharacteristics mc = new MatrixCharacteristics(block.getNumRows(), block.getNumColumns(), 10, block.getNonZeros());
//        MetaData md = new MetaData(mc);
//        MatrixObject mo = new MatrixObject(ValueType.FP64, "", md, block);
//        PartitionedBroadcast<MatrixBlock> res1 = ExecutionUtil.broadcastForMatrixObject(sc, mo);
//        System.out.println("233");
    }
}
