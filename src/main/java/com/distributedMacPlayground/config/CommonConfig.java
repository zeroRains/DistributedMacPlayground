package com.distributedMacPlayground.config;

public class CommonConfig {
    public static String OUTPUT_BUFFER_DIR = "src/test/cache/";
    public static String SYNTHETICDATASET_DIR = "src/main/resources/syntheticDataset/";

    public enum CacheTpye {
        LEFT,
        RIGHT;

        public boolean isRight() {
            return this == RIGHT;
        }

        public CacheTpye getFilped() {
            switch (this) {
                case RIGHT:
                    return LEFT;
                case LEFT:
                    return RIGHT;
                default:
                    return null;
            }
        }
    }

    public enum MMMethodType{
        CpMM, // Cross Product-based Matrix Multiplication
        MapMM, // map-side matrix-matrix multiplication using distributed cache
        PMapMM, // partitioned map-side matrix-matrix multiplication
        PMM, // permutation matrix multiplication using distributed cache
        RMM, // replication matrix multiplication
        ZipMM, // zip matrix multiplication
        CRMM,
        TEST,
    }

    public enum SparkAggType {
        SINGLE_BLOCK,
        MULTI_BLOCK,
        NONE
    }
}
