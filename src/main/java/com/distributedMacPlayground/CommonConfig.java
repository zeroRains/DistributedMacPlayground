package com.distributedMacPlayground;

public class CommonConfig {
    public static String OUTPUT_BUFFER_DIR = "src/test/cache/";

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

    public enum MMMethod{
        CpMM,
        MapMM,

    }

    public enum SparkAggTye {
        SINGLE_BLOCK,
        MULTI_BLOCK,
        NONE
    }
}
