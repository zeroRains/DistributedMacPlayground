package com.distributedMacPlayground.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import scala.Tuple2;

public class RandomMatrixRDDGenerator {
    public double min;
    public double max;
    public double sparse;

    public String pdf; // random type
    public int seed; // random seed

    private int rlen;
    private int clen;
    private int blockSize; // block Size

    public int getRlen() {
        return rlen;
    }

    public int getClen() {
        return clen;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public RandomMatrixRDDGenerator(double min, double max, double sparse, String pdf, int seed) throws Exception {
        this.min = min;
        this.max = max;
        this.sparse = sparse;
        this.pdf = pdf;
        this.seed = seed;
        if (sparse > 1 || sparse < 1)
            throw new Exception("There is some wrong in blockSize or sparse.");
    }

    public RandomMatrixRDDGenerator(double min, double max, double sparse) throws Exception {
        this(min, max, sparse, "uniform", -1);
    }

    public RandomMatrixRDDGenerator(double min, double max, double sparse, String pdf) throws Exception {
        this(min, max, sparse, pdf, -1);
    }

    public RandomMatrixRDDGenerator(double min, double max, double sparse, int seed) throws Exception {
        this(min, max, sparse, "uniform", seed);
    }


    public JavaPairRDD<MatrixIndexes, MatrixBlock> generate(JavaSparkContext sc, String indexFilePath) throws Exception {
        String[] fileName = indexFilePath.split("/");
        String[] tmp = fileName[fileName.length - 1].split("_");
        if (tmp.length < 2)
            throw new Exception("Index file name is wrong. It should be the format like {rlen}x{clen}x{blockSize}_matrix_index.csv");
        else {
            String[] rowAndCol = tmp[0].split("x");
            if (rowAndCol.length != 3)
                throw new Exception("Index file name is wrong. It should be the format like {rlen}x{clen}x{blockSize}_matrix_index.csv");
            else {
                this.rlen = Integer.parseInt(rowAndCol[0]);
                this.clen = Integer.parseInt(rowAndCol[1]);
                this.blockSize = Integer.parseInt(rowAndCol[2]);
            }
        }

        JavaRDD<String> index = sc.textFile(indexFilePath);
        return index.mapToPair(new GeneratePartitionedBlockRDDFunction(rlen, clen, blockSize, min, max, sparse, pdf, seed));
    }

    private static class GeneratePartitionedBlockRDDFunction implements PairFunction<String, MatrixIndexes, MatrixBlock> {
        int rlen;
        int clen;
        int blockSize;
        String pdf;
        int seed;
        double min;
        double max;
        double sparse;

        public GeneratePartitionedBlockRDDFunction(int rlen, int clen, int blockSize, double min, double max, double sparse, String pdf, int seed) {
            this.rlen = rlen;
            this.clen = clen;
            this.blockSize = blockSize;
            this.pdf = pdf;
            this.seed = seed;
            this.min = min;
            this.max = max;
            this.sparse = sparse;
        }


        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> call(String arg0) {
            String[] index = arg0.split(",");
            int rIx = Integer.parseInt(index[0]);
            int cIx = Integer.parseInt(index[1]);
            int rowNum = Math.min(blockSize, rlen - (rIx - 1) * blockSize);
            int colNum = Math.min(blockSize, clen - (cIx - 1) * blockSize);
            MatrixBlock matrixBlock = MatrixBlock.randOperations(rowNum, colNum, sparse, min, max, pdf, seed);
            return new Tuple2<>(new MatrixIndexes(rIx, cIx), matrixBlock);
        }
    }
}
