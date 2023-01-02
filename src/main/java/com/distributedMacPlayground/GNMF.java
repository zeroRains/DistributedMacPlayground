package com.distributedMacPlayground;

import com.distributedMacPlayground.config.CommonConfig;
import com.distributedMacPlayground.method.CpMM;
import com.distributedMacPlayground.method.MapMM;
import com.distributedMacPlayground.method.MatrixMultiply;
import com.distributedMacPlayground.method.RMM;
import com.distributedMacPlayground.operator.Operator;
import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;


public class GNMF {
    static String filePath = null;
    static int row = -1;
    static int col = -1;
    static int blockSize = 1000;
    static int middle = 1;
    static JavaSparkContext sc;

    static int iter = 10;
    static int sparse = 1;
    static int min = 1;
    static int max = 2;
    static String pdf = "uniform";
    static int seed = -1;
    static MatrixMultiply mm = null;
    static DataCharacteristics mc1 = null;
    static DataCharacteristics mc2 = null;
    static DataCharacteristics mc1T = null;
    static DataCharacteristics mc2T = null;
    static DataCharacteristics mcIn = null;
    static DataCharacteristics mcMiddle = null;


    public static void main(String[] args) throws Exception {
        parseParameter(args);
        checkParameter();

        SparkConf conf = new SparkConf().setAppName("GNMF").setMaster("local");
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        if (mm instanceof MapMM) {
            MapMM mapMM = (MapMM) mm;
            mapMM.setSc(sc);
            mm = mapMM;
        }

        JavaPairRDD<MatrixIndexes, MatrixBlock> in = loadData();
        System.out.println("finished load");
        executeGNMF(in);
        System.out.println("finish calculate");
        sc.stop();
    }

    private static void executeGNMF(JavaPairRDD<MatrixIndexes, MatrixBlock> in) throws Exception {
        JavaPairRDD<MatrixIndexes, MatrixBlock> factor1 = generatorMatrixRDD(row, middle, 1);
        JavaPairRDD<MatrixIndexes, MatrixBlock> factor2 = generatorMatrixRDD(middle, col, 2);

        for (int i = 0; i < iter; i++) {
            JavaPairRDD<MatrixIndexes, MatrixBlock> factor11 = updateFactor1(in, factor1, factor2);
            JavaPairRDD<MatrixIndexes, MatrixBlock> factor22 = updateFactor2(in, factor1, factor2);
            MatrixBlock factor1Res = SparkExecutionContext.toMatrixBlock(factor11, row, middle, blockSize, -1);
            MatrixBlock factor2Res = SparkExecutionContext.toMatrixBlock(factor22, middle, col, blockSize, -1);
            factor1 = SparkExecutionContext.toMatrixJavaPairRDD(sc, factor1Res, blockSize);
            factor2 = SparkExecutionContext.toMatrixJavaPairRDD(sc, factor2Res, blockSize);
        }
    }

    private static JavaPairRDD<MatrixIndexes, MatrixBlock> updateFactor2(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in,
            JavaPairRDD<MatrixIndexes, MatrixBlock> factor1,
            JavaPairRDD<MatrixIndexes, MatrixBlock> factor2) throws Exception {
        JavaPairRDD<MatrixIndexes, MatrixBlock> up = Operator.elementWiseProduct(factor2,
                Operator.matrixMultiply(Operator.transpose(factor1), in, mc1T, mcIn, mm));
        JavaPairRDD<MatrixIndexes, MatrixBlock> down = Operator.matrixMultiply(
                Operator.matrixMultiply(Operator.transpose(factor1), factor1, mc1T, mc1, mm),
                factor2, mcMiddle, mc2, mm);
        return Operator.elementWiseDivision(up, down);
    }

    private static JavaPairRDD<MatrixIndexes, MatrixBlock> updateFactor1(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in,
            JavaPairRDD<MatrixIndexes, MatrixBlock> factor1,
            JavaPairRDD<MatrixIndexes, MatrixBlock> factor2) throws Exception {
        JavaPairRDD<MatrixIndexes, MatrixBlock> up = Operator.elementWiseProduct(factor1,
                Operator.matrixMultiply(in, Operator.transpose(factor2), mcIn, mc2T, mm));
        JavaPairRDD<MatrixIndexes, MatrixBlock> down = Operator.matrixMultiply(
                Operator.matrixMultiply(factor1, factor2, mc1, mc2, mm),
                Operator.transpose(factor2), mcIn, mc2T, mm);
        return Operator.elementWiseDivision(up, down);
    }

    private static JavaPairRDD<MatrixIndexes, MatrixBlock> generatorMatrixRDD(int r, int c, int n) {
        int rowBlk = (int) Math.ceil(1.0 * r / blockSize);
        int colBlk = (int) Math.ceil(1.0 * c / blockSize);
        List<MatrixIndexes> list = new ArrayList<>();
        for (int i = 1; i <= rowBlk; i++)
            for (int j = 1; j <= colBlk; j++)
                list.add(new MatrixIndexes(i, j));
        return sc.parallelize(list).mapToPair(new generatorRDDMatrixFunction(r, c, n));
    }

    private static JavaPairRDD<MatrixIndexes, MatrixBlock> loadData() {
        MatrixCharacteristics mc = new MatrixCharacteristics(row, col, blockSize);
        mcIn = mc;
        mc1 = new MatrixCharacteristics(row, middle, blockSize);
        mc1T = new MatrixCharacteristics(middle, row, blockSize);
        mc2 = new MatrixCharacteristics(middle, col, blockSize);
        mc2T = new MatrixCharacteristics(col, middle, blockSize);
        mcMiddle = new MatrixCharacteristics(middle, middle, blockSize);
        MatrixBlock mb;
        if (filePath != null) {
            mb = IOUtil.loadMatrixMarketFileFromHDFS(filePath, mc);
        } else {
            mb = MatrixBlock.randOperations(row, col, 1, 2, 3, "uniform", 1024);
        }

        return SparkExecutionContext.toMatrixJavaPairRDD(sc, mb, blockSize);
    }


    private static void checkParameter() throws Exception {
        if (col == -1 || row == -1 || mm == null)
            throw new Exception("You must provide the '-col', '-type', and '-row' parameter.");
    }

    public static void parseParameter(String[] args) throws Exception {
        if (args.length % 2 != 0) throw new Exception("Some parameter have no value!");
        for (int i = 0; i < args.length; i += 2) {
            switch (args[i].toUpperCase()) {
                case "-TYPE":
                    switch (args[i + 1].toUpperCase()) {
                        case "CPMM":
                            mm = new CpMM();
                            break;
                        case "MAPMM":
                            mm = new MapMM();
                            break;
                        case "RMM":
                            mm = new RMM();
                            break;
                        default:
                            throw new Exception("have not supported this method!");
                    }
                    break;
                case "-PATH":
                    filePath = args[i + 1];
                    break;
                case "-COL":
                    col = Integer.parseInt(args[i + 1]);
                    break;
                case "-ROW":
                    row = Integer.parseInt(args[i + 1]);
                    break;
                case "-MIDDLE":
                    middle = Integer.parseInt(args[i + 1]);
                    break;
                case "-BLOCKSIZE":
                    blockSize = Integer.parseInt(args[i + 1]);
                    break;
                default:
                    throw new Exception("We have not supported this parameter");
            }
        }
    }

    private static class generatorRDDMatrixFunction implements PairFunction<MatrixIndexes, MatrixIndexes, MatrixBlock> {
        int r;
        int c;
        int name;

        public generatorRDDMatrixFunction(int r, int c, int n) {
            this.r = r;
            this.c = c;
            name = n;
        }

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(MatrixIndexes arg) throws Exception {
            int rowIndex = (int) arg.getRowIndex();
            int colIndex = (int) arg.getColumnIndex();
            int rNum = rowIndex * blockSize > r ? r - (rowIndex - 1) * blockSize : blockSize;
            int cNum = colIndex * blockSize > c ? c - (colIndex - 1) * blockSize : blockSize;
            MatrixBlock mb = MatrixBlock.randOperations(rNum, cNum, sparse, min, max, pdf, seed);
            return new Tuple2<>(arg, mb);
        }
    }
}
