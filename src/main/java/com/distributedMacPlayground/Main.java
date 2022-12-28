package com.distributedMacPlayground;

import com.distributedMacPlayground.util.RandomMatrixRDDGenerator;
import com.distributedMacPlayground.util.TimeStatisticsUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.distributedMacPlayground.CommonConfig.MMMethodType;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
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
    static String saveFilePath = null;
    static MMMethodType _type = null;
    static CommonConfig.CacheTpye _cacheType = CommonConfig.CacheTpye.LEFT;
    static CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;


    public static void main(String[] args) throws Exception {
        // 1. create spark environment
        SparkConf sparkConf = new SparkConf().setAppName("test"); //you can use .setMaster("local") to run in the local machine when you test the program.
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        TimeStatisticsUtil.totalStart(System.nanoTime());
        // 2. parse and check the parameters
        TimeStatisticsUtil.parametersCheckStart(System.nanoTime());
        parseParameter(args);
        checkParameters();
        TimeStatisticsUtil.parametersCheckStop(System.nanoTime());


        // 3. RDD transform and execute the distributed matrix multiply

        if (dataType.equals("data")) {
            if (_type != MMMethodType.MapMM) {
                TimeStatisticsUtil.loadDataStart(System.nanoTime());
                RunMethod runMethod = new RunMethod(sc, _type, row, col, middle, blockSize, in1Path, in2Path);
                runMethod.set_tRewrite(_tWrite);
                TimeStatisticsUtil.loadDataStop(System.nanoTime());

                TimeStatisticsUtil.calculateStart(System.nanoTime());
                runMethod.execute();
                if (runMethod.getOut() != null) {
                    List<Tuple2<MatrixIndexes, MatrixBlock>> res = runMethod.getOut().collect();
                }
                TimeStatisticsUtil.calculateStop(System.nanoTime());

            } else {
                TimeStatisticsUtil.loadDataStart(System.nanoTime());
                RunMethod runMethod = new RunMethod(sc, _type, row, col, middle, blockSize, _cacheType, _aggType, in1Path, in2Path);
                runMethod.set_outputEmpty(outputEmpty);
                TimeStatisticsUtil.loadDataStop(System.nanoTime());

                TimeStatisticsUtil.calculateStart(System.nanoTime());
                runMethod.execute();
                TimeStatisticsUtil.calculateStop(System.nanoTime());
            }
        } else {
            TimeStatisticsUtil.loadDataStart(System.nanoTime());
            RandomMatrixRDDGenerator rddGenerator = new RandomMatrixRDDGenerator(min, max, sparsity, pdf, seed);
            JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = rddGenerator.generate(sc, in1Path);
            row = rddGenerator.getRlen();
            middle = rddGenerator.getClen();
            blockSize = rddGenerator.getBlockSize();
            JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = rddGenerator.generate(sc, in2Path);
            col = rddGenerator.getClen();
            if (middle != rddGenerator.getRlen())
                throw new Exception("Dimension do not match!");
            if (_type != MMMethodType.MapMM) {
                RunMethod runMethod = new RunMethod(sc, _type, row, col, middle, blockSize, in1, in2);
                runMethod.set_tRewrite(_tWrite);
                TimeStatisticsUtil.loadDataStop(System.nanoTime());

                TimeStatisticsUtil.calculateStart(System.nanoTime());
                if (_type == MMMethodType.TEST && saveFilePath != null) {
                    runMethod.setOutputIn1Path(saveFilePath + "/" + row + "x" + middle + "x" + blockSize + "_matrix_data.csv");
                    if (row != middle)
                        runMethod.setOutputIn2Path(saveFilePath + "/" + middle + "x" + col + "x" + blockSize + "_matrix_data.csv");
                }
                runMethod.execute();
                if (runMethod.getOut() != null && _type != MMMethodType.TEST) {
                    List<Tuple2<MatrixIndexes, MatrixBlock>> tmp = runMethod.getOut().collect();
                }
                TimeStatisticsUtil.calculateStop(System.nanoTime());
            } else {
                RunMethod runMethod = new RunMethod(sc, _type, row, col, middle, blockSize, _cacheType, _aggType, in1, in2);
                runMethod.set_outputEmpty(outputEmpty);
                TimeStatisticsUtil.loadDataStop(System.nanoTime());

                TimeStatisticsUtil.calculateStart(System.nanoTime());
                runMethod.execute();
                TimeStatisticsUtil.calculateStop(System.nanoTime());
            }
        }
        TimeStatisticsUtil.totalTimeStop(System.nanoTime());
        // 4. output the execution time
        System.out.println("Default parallelism:   " + sc.defaultParallelism());
        System.out.println("Check parameters time: " + String.format("%.9f", TimeStatisticsUtil.getParametersCheckTime()) + " s.");
        System.out.println("Load data time:        " + String.format("%.9f", TimeStatisticsUtil.getLoadDataTime()) + " s.");
        System.out.println("Calculate time:        " + String.format("%.9f", TimeStatisticsUtil.getCalculateTime()) + " s.");
        System.out.println("Total time:            " + String.format("%.9f", TimeStatisticsUtil.getTotalTime()) + " s.");
        System.out.println("Finish to calculate distributed matrix multiply.");

        // 5. close spark environment
        sc.close(); // if you didn't write it, your application state will be FAILED.
    }

    public static void checkParameters() throws Exception {
        if (_type == null || dataType == null || in1Path == null || in2Path == null
                || (dataType.equals("data") && (row == -1 || middle == -1 || col == -1 || blockSize == -1)))
            throw new Exception("You must provide the follow parameters: -mmType -dataType -in1 -in2.\n" +
                    "if the value of -dataType is data, you also need to provide parameters: -row -middle -col -blockSize");
        if (_type == MMMethodType.TEST && !dataType.equals("index"))
            throw new Exception("Only dataType == 'index' can use the MMType == 'test' ");
    }

    public static void parseParameter(String[] args) throws Exception {
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
                        case "CRMM":
                            _type = MMMethodType.CRMM;
                            break;
                        case "TEST":
                            _type = MMMethodType.TEST;
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
                case "-SAVE":
                    saveFilePath = args[i + 1];
                    break;
                default:
                    throw new Exception("there do not support parameter called " + args[i]);
            }
        }
    }
}