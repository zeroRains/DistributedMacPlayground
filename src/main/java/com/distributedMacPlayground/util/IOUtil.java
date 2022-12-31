package com.distributedMacPlayground.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.data.SparseRow;
import org.apache.sysds.runtime.instructions.spark.utils.RDDConverterUtils;
import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class IOUtil {
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

    public static JavaPairRDD<MatrixIndexes, MatrixBlock> csvFileToMatrixRDD(JavaSparkContext sc, String path, DataCharacteristics mc) {
        JavaRDD<String> csvData = sc.textFile(path);
        return RDDConverterUtils.csvToBinaryBlock(sc, csvData, mc, false, ",", true, 0, null);
    }

    public static void saveMatrixAsCSVFile(JavaPairRDD<MatrixIndexes, MatrixBlock> in, String path, DataCharacteristics mc) {
        JavaRDD<String> csvData = RDDConverterUtils.binaryBlockToCsv(in, mc, new FileFormatPropertiesCSV(), true);
        csvData.saveAsTextFile(path); // merge to 1 file
    }

    public static MatrixBlock loadMatrixMarketFileFromHDFS(String path,DataCharacteristics mc){
        MatrixObject mo = new MatrixObject(Types.ValueType.FP64, path);
        MetaDataFormat mdf = new MetaDataFormat(mc, Types.FileFormat.MM);
        mo.setMetaData(mdf);
        return mo.acquireRead();
    }


}
