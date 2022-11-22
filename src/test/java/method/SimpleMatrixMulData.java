package method;

import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;

public class SimpleMatrixMulData {
    public MatrixBlock in1Block;
    public MatrixBlock in2Block;
    public MatrixBlock in3Block;
    public DataCharacteristics mc1;
    public DataCharacteristics mc2;
    public DataCharacteristics mc3;

    public SimpleMatrixMulData(int row1, int col1, int row2, int col2, double sparsity1, double sparsity2, int min1, int min2, int max1, int max2, String pdf, int seed, int blen) {
        in1Block = MatrixBlock.randOperations(row1, col1, sparsity1, min1, max1, pdf, seed);
        in2Block = MatrixBlock.randOperations(row2, col2, sparsity2, min2, max2, pdf, seed);
        in3Block = MatrixBlock.randOperations(col2, row1, sparsity2, min2, max2, pdf, seed);
        mc1 = new MatrixCharacteristics(in1Block.getNumRows(), in1Block.getNumColumns(), blen, in1Block.getNonZeros());
        mc2 = new MatrixCharacteristics(in2Block.getNumRows(), in2Block.getNumColumns(), blen, in2Block.getNonZeros());
        mc3 = new MatrixCharacteristics(in3Block.getNumRows(), in3Block.getNumColumns(), blen, in3Block.getNonZeros());
    }
}
