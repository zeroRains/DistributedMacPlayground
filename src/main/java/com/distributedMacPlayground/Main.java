package com.distributedMacPlayground;

import com.distributedMacPlayground.util.RandomMatrixRDDGenerator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.distributedMacPlayground.CommonConfig.MMMethodType;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import scala.Tuple2;

import java.util.List;


public class Main {


    static int row = -1;
    static int middle = -1;
    static int col = -1;
    static int blockSize = -1;
    static int seed = -1;
    static double min = 5;
    static double max = 10;
    static double sparsity = 1;
    static boolean _tWrite = true;
    static boolean outputEmpty = false;
    static String dataType = null;
    static String in1Path = null;
    static String in2Path = null;
    static String pdf = "uniform";
    static MMMethodType _type = null;
    static CommonConfig.CacheTpye _cacheType = CommonConfig.CacheTpye.LEFT;
    static CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;


    public static void main(String[] args) throws Exception {
        parseParameter(args);
        checkParameters();
        SparkConf sparkConf = new SparkConf().setAppName("test"); //you can use .setMaster("local") to run in the local machine when you test the program.
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        if (dataType.equals("data")) {
            if (_type != MMMethodType.MapMM) {
                RunMethod runMethod = new RunMethod(sc, _type, row, col, blockSize, middle, in1Path, in2Path);
                runMethod.execute();
                if (runMethod.getOut() != null) {
                    List<Tuple2<MatrixIndexes, MatrixBlock>> res = runMethod.getOut().collect();
                }
            } else {
                RunMethod runMethod = new RunMethod(sc, _type, row, col, middle, blockSize, _cacheType, _aggType, in1Path, in2Path);
                runMethod.execute();
            }
        } else {
            if (_type != MMMethodType.MapMM) {
                RandomMatrixRDDGenerator rddGenerator = new RandomMatrixRDDGenerator(min, max, sparsity, pdf, seed);
                JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = rddGenerator.generate(sc, in1Path);
                row = rddGenerator.getRlen();
                middle = rddGenerator.getClen();
                blockSize = rddGenerator.getBlockSize();
                JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = rddGenerator.generate(sc, in2Path);
                col = rddGenerator.getClen();
                RunMethod runMethod = new RunMethod(sc, _type, row, col, middle, blockSize, in1, in2);
                runMethod.execute();
                if (runMethod.getOut() != null) {
                    List<Tuple2<MatrixIndexes, MatrixBlock>> tmp = runMethod.getOut().collect();
                }
            } else
                throw new Exception("I have not wrote!");
        }
        System.out.println("finished");
        sc.close(); // if you didn't write it, your application state will be FAILED.
    }

    public static void checkParameters() throws Exception {
        if (_type == null || dataType == null || in1Path == null || in2Path == null
                || (dataType.equals("data") && (row == -1 || middle == -1 || col == -1 || blockSize == -1)))
            throw new Exception("You must provide the follow parameters: -mmType -dataType -in1 -in2.\n" +
                    "if the value of -dataType is data, you also need to provide parameters: -row -middle -col -blockSize");
    }

    public static void parseParameter(String[] args) throws Exception {
        // hdfs test parameters:
        //  -mmType CpMM -dataType data -in1 hdfs://localhost:9000/user/root/test/in1.csv -in2 hdfs://localhost:9000/user/root/test/in2.csv -row 100 -col 200 -middle 300 -blockSize 10
        // -mmtype CpMM -datatype index -in1 hdfs://localhost:9000/user/root/test/100x300x10_matrix_index.csv -in2 hdfs://localhost:9000/user/root/test/300x100x10_matrix_index.csv
        // -mmtype MapMM -datatype data -in1 hdfs://localhost:9000/user/root/test/in1.csv -in2 hdfs://localhost:9000/user/root/test/in2.csv -cacheType left -aggType multi -row 100 -col 200 -middle 300 -blockSize 10
        // -mmtype MapMM -datatype data -in1 hdfs://localhost:9000/user/root/test/100x300x10_matrix_index.csv -in2 hdfs://localhost:9000/user/root/test/300x100x10_matrix_index.csv -cacheType left -aggType multi
        // -twrite true -outputEmpty false

        // local test parameters:
        // -mmType CpMM -dataType data -blockSize 10 -row 100 -col 1 -middle 200 -in1 src/test/cache/Cpmm/in1.csv -in2 src/test/cache/Cpmm/in2.csv
        // -mmType MapMM -dataType data -blockSize 10 -row 100 -col 1  -middle 200 -cacheType left -aggType multi -in1 src/test/cache/Cpmm/in1.csv -in2 src/test/cache/Cpmm/in2.csv
        // -mmType CpMM -dataType index -in1 src/main/resources/syntheticDataset/100x300x10_matrix_index.csv -in2 src/main/resources/syntheticDataset/300x100x10_matrix_index.csv

        // -mmType CpMM -dataType index -cacheType left -aggType multi -in1 src/main/resources/syntheticDataset/100x300x10_matrix_index.csv -in2 src/main/resources/syntheticDataset/300x100x10_matrix_index.csv

        if (args.length % 2 != 0) throw new Exception("Some parameter have no value!");
        for (int i = 0; i < args.length; i += 2) {
            switch (args[i].toUpperCase()) {
                case "-MMTYPE":
                    switch (args[i + 1].toUpperCase()) {
                        case "CPMM":
                            _type = MMMethodType.CpMM;
                            break;
                        case "MAPMM":
                            _type = MMMethodType.MapMM;
                            break;
                        case "PMAPMM":
                            _type = MMMethodType.PMapMM;
                            break;
                        case "PMM":
                            _type = MMMethodType.PMM;
                            break;
                        case "RMM":
                            _type = MMMethodType.RMM;
                            break;
                        case "ZIPMM":
                            _type = MMMethodType.ZipMM;
                            break;
                        default:
                            throw new Exception("have not supported this method!");
                    }
                    break;
                case "-DATATYPE":
                    dataType = args[i + 1];
                    break;
                case "-IN1":
                    in1Path = args[i + 1];
                    break;
                case "-IN2":
                    in2Path = args[i + 1];
                    break;
                case "-ROW":
                    row = Integer.parseInt(args[i + 1]);
                    break;
                case "-COL":
                    col = Integer.parseInt(args[i + 1]);
                    break;
                case "-MIDDLE":
                    middle = Integer.parseInt(args[i + 1]);
                    break;
                case "-BLOCKSIZE":
                    blockSize = Integer.parseInt(args[i + 1]);
                    break;
                case "-CACHETYPE":
                    switch (args[i + 1].toUpperCase()) {
                        case "LEFT":
                            _cacheType = CommonConfig.CacheTpye.LEFT;
                            break;
                        case "RIGHT":
                            _cacheType = CommonConfig.CacheTpye.RIGHT;
                        default:
                            throw new Exception("There do not support " + args[i + 1] + " in cacheType");
                    }
                    break;
                case "-AGGTYPE":
                    switch (args[i + 1].toUpperCase()) {
                        case "MULTI":
                            _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;
                            break;
                        case "SINGLE":
                            _aggType = CommonConfig.SparkAggType.SINGLE_BLOCK;
                            break;
                        default:
                            throw new Exception("There do not support " + args[i + 1] + " in aggType!");
                    }
                    break;
                case "-TWRITE":
                    _tWrite = Boolean.parseBoolean(args[i + 1]);
                    break;
                case "-OUTPUTEMPTY":
                    outputEmpty = Boolean.parseBoolean(args[i + 1]);
                    break;
                case "-MIN":
                    min = Double.parseDouble(args[i + 1]);
                    break;
                case "-MAX":
                    max = Double.parseDouble(args[i + 1]);
                    break;
                case "-SPARSITY":
                    sparsity = Double.parseDouble(args[i + 1]);
                    if (sparsity < 0 || sparsity > 1)
                        throw new Exception("sparsity need to range in [0,1]");
                    break;
                case "-SEED":
                    seed = Integer.parseInt(args[i + 1]);
                    break;
                case "-PDF":
                    pdf = args[i + 1];
                    break;
                default:
                    throw new Exception("there do not support parameter called " + args[i]);
            }
        }
    }
}