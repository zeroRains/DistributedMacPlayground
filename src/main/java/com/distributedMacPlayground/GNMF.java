package com.distributedMacPlayground;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class GNMF {
    public static void main(String[] args) {
        String path = "H:\\datasets\\matrix_multiplication\\soc-buzznet\\soc-buzznet.mtx";
        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

    }
}
