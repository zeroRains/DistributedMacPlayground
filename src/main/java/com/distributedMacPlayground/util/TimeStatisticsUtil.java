package com.distributedMacPlayground.util;

public class TimeStatisticsUtil {
    private static long totalTimeStart;
    private static long totalTimeEnd;
    private static long loadDataStart;
    private static long loadDataEnd;

    private static long calculateStart;
    private static long calculateEnd;

    private static long parametersCheckStart;
    private static long parametersCheckEnd;

    public static double NANOSECOND_TO_SECOND = 1000000000.0;

    public static void loadDataStart(long t) {
        loadDataStart = t;
    }

    public static void loadDataStop(long t) {
        loadDataEnd = t;
    }

    public static void calculateStart(long t) {
        calculateStart = t;
    }

    public static void calculateStop(long t) {
        calculateEnd = t;
    }

    public static void parametersCheckStart(long t) {
        parametersCheckStart = t;
    }

    public static void parametersCheckStop(long t) {
        parametersCheckEnd = t;
    }

    public static void totalStart(long t) {
        totalTimeStart = t;
    }

    public static void totalTimeStop(long t) {
        totalTimeEnd = t;
    }

    public static double getLoadDataTime() {
        return (loadDataEnd - loadDataStart) / NANOSECOND_TO_SECOND;
    }


    public static double getCalculateTime() {
        return (calculateEnd - calculateStart) / NANOSECOND_TO_SECOND;
    }

    public static double getParametersCheckTime() {
        return (parametersCheckEnd - parametersCheckStart) / NANOSECOND_TO_SECOND;
    }

    public static double getTotalTime() {
        return (totalTimeEnd - totalTimeStart) / NANOSECOND_TO_SECOND;
    }
}
