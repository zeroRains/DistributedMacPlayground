package com.distributedMacPlayground.util;

import org.apache.sysds.runtime.data.SparseRow;
import org.apache.sysds.runtime.data.SparseRowVector;
import org.apache.sysds.runtime.matrix.data.IJV;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Iterator;

public class OutputUtil {
    /**
     * output MatrixBlock to a local CSV file
     *
     * @param path   output file path
     * @param matrix matrixBlock for output
     * @throws Exception
     */
    public static void outputMatrixToLocalCSV(String path, MatrixBlock matrix) throws Exception {
        if (matrix.isEmpty() || matrix.isEmptyBlock())
            throw new Exception("It is a empty matrix!");
        BufferedWriter out = new BufferedWriter(new FileWriter(path));
        boolean isSparse = matrix.isInSparseFormat();
        int col = matrix.getNumColumns();
        int row = matrix.getNumRows();

        if (isSparse) {
            for (int i = 0; i < row; i++) {
                StringBuffer sb = new StringBuffer();
                if (i != 0) sb.append("\n");
                SparseRow rowData = matrix.getSparseBlock().get(i);
                int[] location = rowData.indexes();
                int cnt = 0;
                for (int j = 0; j < col; j++) {
                    if (j != 0) sb.append(",");
                    if (cnt < location.length && j == location[cnt]) {
                        sb.append(rowData.get(j));
                        cnt++;
                    } else sb.append(0);
                }
                out.write(sb.toString());
            }

        } else {
            double[] data = matrix.getDenseBlockValues();
            if (col * row != data.length) {
                out.close();
                throw new Exception("Dimension Error!");
            }
            for (int i = 0; i < row; i++) {
                StringBuffer sb = new StringBuffer();
                if (i != 0) sb.append("\n");
                for (int j = 0; j < col; j++) {
                    if (j != 0) sb.append(",");
                    sb.append(data[i * col + j]);
                }
                out.write(sb.toString());
            }
        }
        out.close();
    }
    
}
