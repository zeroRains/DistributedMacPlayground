package com.distributedMacPlayground;

import com.distributedMacPlayground.operator.Operator;
import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
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
    static int blockSize = -1;
    static int middle = 1;
    static JavaSparkContext sc;

    static int iter = 100000;
    static int sparse = 1;
    static int min = 1;
    static int max = 2;
    static String pdf = "uniform";
    static int seed = -1;


    public static void main(String[] args) throws Exception {
        parseParameter(args);
        checkParameter();

        SparkConf conf = new SparkConf().setAppName("GNMF");
        sc = new JavaSparkContext(conf);

        JavaPairRDD<MatrixIndexes, MatrixBlock> in = loadData();
        executeGNMF(in);
    }

    private static void executeGNMF(JavaPairRDD<MatrixIndexes, MatrixBlock> in) {
        int rowBlockNum = (int) Math.ceil(1.0 * row / blockSize);
        int colBlockNum = (int) Math.ceil(1.0 * col / blockSize);
        int middleBlockNum = (int) Math.ceil(1.0 * middle / blockSize);
        JavaPairRDD<MatrixIndexes, MatrixBlock> factor1 = generatorMatrixRDD(rowBlockNum, middleBlockNum);
        JavaPairRDD<MatrixIndexes, MatrixBlock> factor2 = generatorMatrixRDD(middleBlockNum, colBlockNum);

        for (int i = 0; i < iter; i++) {
            factor1 = updateFactor1(in, factor1, factor2);
            factor2 = updateFactor2(in, factor1, factor2);
        }

        ForkJoinPool pool = new ForkJoinPool();
        JavaPairRDD<MatrixIndexes, MatrixBlock> finalFactor = factor1;
        pool.submit(() -> {
            long res = finalFactor.count();
            System.out.println("get the factor1 result!" + res);
        });
        JavaPairRDD<MatrixIndexes, MatrixBlock> finalFactor1 = factor2;
        pool.submit(() -> {
            long res = finalFactor1.count();
            System.out.println("get the factor2 result!" + res);
        });
        pool.awaitQuiescence(1, TimeUnit.MINUTES);

        pool.shutdown();
    }

    private static JavaPairRDD<MatrixIndexes, MatrixBlock> updateFactor2(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in,
            JavaPairRDD<MatrixIndexes, MatrixBlock> factor1,
            JavaPairRDD<MatrixIndexes, MatrixBlock> factor2) {
        JavaPairRDD<MatrixIndexes, MatrixBlock> up = Operator.elementWiseMultiply(factor2,
                Operator.matrixMultiply(Operator.transpose(factor1), in));
        JavaPairRDD<MatrixIndexes, MatrixBlock> down = Operator.matrixMultiply(
                Operator.matrixMultiply(Operator.transpose(factor1), factor1),
                factor2);
        return Operator.elementWiseDivision(up, down);
    }

    private static JavaPairRDD<MatrixIndexes, MatrixBlock> updateFactor1(
            JavaPairRDD<MatrixIndexes, MatrixBlock> in,
            JavaPairRDD<MatrixIndexes, MatrixBlock> factor1,
            JavaPairRDD<MatrixIndexes, MatrixBlock> factor2) {
        JavaPairRDD<MatrixIndexes, MatrixBlock> up = Operator.elementWiseMultiply(factor1,
                Operator.matrixMultiply(in, Operator.transpose(factor2)));
        JavaPairRDD<MatrixIndexes, MatrixBlock> down = Operator.matrixMultiply(
                Operator.matrixMultiply(factor1, factor2), Operator.transpose(factor2));
        return Operator.elementWiseDivision(up, down);
    }

    private static JavaPairRDD<MatrixIndexes, MatrixBlock> generatorMatrixRDD(int r, int c) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < r * c; i++)
            list.add(i);
        return sc.parallelize(list).mapToPair(new generatorRDDMatrixFunction());
    }

    private static JavaPairRDD<MatrixIndexes, MatrixBlock> loadData() {
        MatrixCharacteristics mc = new MatrixCharacteristics(row, col, blockSize);
        MatrixBlock mb;
        if (filePath != null) {
            mb = IOUtil.loadMatrixMarketFileFromHDFS(filePath, mc);
        } else {
            mb = MatrixBlock.randOperations(row, col, 1, 2, 3, "uniform", 1024);
        }
        return SparkExecutionContext.toMatrixJavaPairRDD(sc, mb, blockSize);
    }


    private static void checkParameter() throws Exception {
        if (col == -1 || row == -1 || blockSize == -1)
            throw new Exception("You must provide the '-col', '-row' and '-blocksize' parameter.");
    }

    public static void parseParameter(String[] args) throws Exception {
        if (args.length % 2 != 0) throw new Exception("Some parameter have no value!");
        for (int i = 0; i < args.length; i += 2) {
            switch (args[i].toUpperCase()) {
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

    private static class generatorRDDMatrixFunction implements PairFunction<Integer, MatrixIndexes, MatrixBlock> {
        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(Integer arg) throws Exception {
            int rowIndex = arg / blockSize + 1;
            int colIndex = arg % blockSize + 1;
            MatrixIndexes mi = new MatrixIndexes(rowIndex, colIndex);
            MatrixBlock mb = MatrixBlock.randOperations(blockSize, blockSize, sparse, min, max, pdf, seed);
            return new Tuple2<>(mi, mb);
        }
    }
}
