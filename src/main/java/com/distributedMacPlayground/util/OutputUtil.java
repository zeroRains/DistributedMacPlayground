package com.distributedMacPlayground.util;

import org.apache.sysds.runtime.matrix.data.MatrixBlock;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.Buffer;

public class OutputUtil {

    public static void outputMatrixToLocalCSV(String path, MatrixBlock matrix) throws Exception {
        if (matrix.isEmpty() || matrix.isEmptyBlock())
            throw new Exception("It is a empty matrix!");
        BufferedWriter out = new BufferedWriter(new FileWriter(path));
        boolean isSparse = matrix.isInSparseFormat();
        int col = matrix.getNumColumns();
        int row = matrix.getNumRows();

        if (isSparse) {

        } else {
            double[] data = matrix.getDenseBlockValues();
            if (col * row != data.length)
                throw new Exception("Dimension Error!");
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
