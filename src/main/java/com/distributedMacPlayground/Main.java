package com.distributedMacPlayground;


import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import com.distributedMacPlayground.CommonConfig.MMMethodType;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import scala.Tuple2;

import java.util.List;
import java.util.Objects;

public class Main {
    static String path = "hdfs://localhost:9000/user/root/test/";
    static MMMethodType _type = MMMethodType.CpMM;
    static int row = 100;
    static int middle = 200;
    static int col = 1;
    static int blockSize = 10;


    public static void main(String[] args) throws Exception {
        String type = args[0];
        String in1 = args.length == 3 ? args[1] : null;
        String in2 = args.length == 3 ? args[2] : null;
        CommonConfig.CacheTpye _cacheType = CommonConfig.CacheTpye.LEFT;
        CommonConfig.SparkAggType _aggType = CommonConfig.SparkAggType.MULTI_BLOCK;

        SparkConf sparkConf = new SparkConf().setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        switch (type) {
            case "CpMM":
                _type = MMMethodType.CpMM;
                break;
            case "MapMM":
                _type = MMMethodType.MapMM;
                if (args.length == 5) {
                    in1 = args[1];
                    in2 = args[2];
                    _cacheType = args[3].equals("left") ? CommonConfig.CacheTpye.LEFT : CommonConfig.CacheTpye.RIGHT;
                    _aggType = args[4].equals("multi_block") ? CommonConfig.SparkAggType.MULTI_BLOCK : CommonConfig.SparkAggType.SINGLE_BLOCK;
                }
                break;
            case "PMapMM":
                _type = MMMethodType.PMapMM;
                break;
            case "PMM":
                _type = MMMethodType.PMM;
                break;
            case "RMM":
                _type = MMMethodType.RMM;
                break;
            case "ZipMM":
                _type = MMMethodType.ZipMM;
                break;
            default:
                throw new Exception("have not supported this method!");
        }


        if (in1 != null && in2 != null) {
            if (_type != MMMethodType.MapMM) {
                RunMethod runMethod = new RunMethod(sc, _type, row, col, middle, path + in1, path + in2);
                runMethod.set_blockSize(blockSize);
                runMethod.execute();
                if (runMethod.getOut() != null) {
                    List<Tuple2<MatrixIndexes, MatrixBlock>> res = runMethod.getOut().collect();
                }
            } else {
                RunMethod runMethod = new RunMethod(sc, _type, row, col, middle, _cacheType, _aggType, path + in1, path + in2);
                runMethod.set_blockSize(blockSize);
                runMethod.execute();
            }
        } else {
            throw new Exception("I have not wrote!");
        }
        System.out.println("finished");
        sc.close(); // if you didn't write it, your application state will be FAILED.
    }
}