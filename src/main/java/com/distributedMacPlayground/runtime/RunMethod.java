package com.distributedMacPlayground.runtime;

import com.distributedMacPlayground.config.CommonConfig.MMMethodType;
import com.distributedMacPlayground.config.CommonConfig.CacheTpye;
import com.distributedMacPlayground.config.CommonConfig.SparkAggType;
import com.distributedMacPlayground.method.*;
import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;

public class RunMethod {
    JavaSparkContext sc;
    MMMethodType _methodType = null; // type of matrix multiplication
    int _outRowLen = -1;
    int _outColLen = -1;
    int _middleLen = -1;

    CacheTpye _cacheType = null;
    SparkAggType _aggType = null;
    JavaPairRDD<MatrixIndexes, MatrixBlock> in1;
    JavaPairRDD<MatrixIndexes, MatrixBlock> in2;
    String matrixPath1;
    String matrixPath2;
    DataCharacteristics mc1;
    DataCharacteristics mc2;
    MatrixBlock blkIn1 = null;
    MatrixBlock blkIn2 = null;

    // get data from csv data file (without header), expect MapMM
    public RunMethod(JavaSparkContext sc, MMMethodType _methodType, int _outRowLen, int _outColLen, int _middleLen, int _blockSize, String matrixPath1, String matrixPath2) {
        this.sc = sc;
        this._methodType = _methodType;
        this._outRowLen = _outRowLen;
        this._outColLen = _outColLen;
        this._middleLen = _middleLen;
        this.matrixPath1 = matrixPath1;
        this.matrixPath2 = matrixPath2;
        this._blockSize = _blockSize;
        this.mc1 = new MatrixCharacteristics(_outRowLen, _middleLen, _blockSize);
        this.mc2 = new MatrixCharacteristics(_middleLen, _outColLen, _blockSize);
        in1 = IOUtil.csvFileToMatrixRDD(this.sc, this.matrixPath1, this.mc1); // read the matrix data from a .csv file
        if (this.matrixPath1.equals(this.matrixPath2)) {
            in1.persist(StorageLevel.MEMORY_ONLY());
            in2 = in1;
        } else {
            in2 = IOUtil.csvFileToMatrixRDD(this.sc, this.matrixPath2, this.mc2);
        }
    }

    // get data by index， expect MapMM
    public RunMethod(JavaSparkContext sc, MMMethodType _methodType, int _outRowLen, int _outColLen, int _middleLen, int _blockSize, JavaPairRDD<MatrixIndexes, MatrixBlock> in1, JavaPairRDD<MatrixIndexes, MatrixBlock> in2) {
        this.sc = sc;
        this._methodType = _methodType;
        this._outRowLen = _outRowLen;
        this._outColLen = _outColLen;
        this._middleLen = _middleLen;
        this._blockSize = _blockSize;
        this.in1 = in1;
        this.in2 = in2;
        this.mc1 = new MatrixCharacteristics(_outRowLen, _middleLen, _blockSize);
        this.mc2 = new MatrixCharacteristics(_middleLen, _outColLen, _blockSize);
    }

    // get data from csv file (without header), use for MapMM
    public RunMethod(JavaSparkContext sc, MMMethodType _methodType, int _outRowLen, int _outColLen, int _middleLen, int _blockSize, CacheTpye _cacheType, SparkAggType _aggType, String matrixPath1, String matrixPath2) {
        this.sc = sc;
        this._methodType = _methodType;
        this._outRowLen = _outRowLen;
        this._outColLen = _outColLen;
        this._middleLen = _middleLen;
        this._cacheType = _cacheType;
        this._aggType = _aggType;
        this.matrixPath1 = matrixPath1;
        this.matrixPath2 = matrixPath2;
        this._blockSize = _blockSize;
        this.mc1 = new MatrixCharacteristics(_outRowLen, _middleLen, _blockSize);
        this.mc2 = new MatrixCharacteristics(_middleLen, _outColLen, _blockSize);
        in1 = IOUtil.csvFileToMatrixRDD(this.sc, this.matrixPath1, this.mc1); // read the matrix data from a .csv file
        if (this.matrixPath1.equals(this.matrixPath2)) {
            in1.persist(StorageLevel.MEMORY_ONLY());
            in2 = in1;
        } else {
            in2 = IOUtil.csvFileToMatrixRDD(this.sc, this.matrixPath2, this.mc2);
        }
    }

    public RunMethod(JavaSparkContext sc, MMMethodType _methodType, int _outRowLen, int _outColLen, int _middleLen, int _blockSize, CacheTpye _cacheType, SparkAggType _aggType, JavaPairRDD<MatrixIndexes, MatrixBlock> in1, JavaPairRDD<MatrixIndexes, MatrixBlock> in2) {
        this.sc = sc;
        this.in1 = in1;
        this.in2 = in2;
        this._methodType = _methodType;
        this._outRowLen = _outRowLen;
        this._outColLen = _outColLen;
        this._middleLen = _middleLen;
        this._cacheType = _cacheType;
        this._aggType = _aggType;
        this._blockSize = _blockSize;
        this.mc1 = new MatrixCharacteristics(_outRowLen, _middleLen, _blockSize);
        this.mc2 = new MatrixCharacteristics(_middleLen, _outColLen, _blockSize);
    }

    private MatrixMultiply mm = null;
    private final int _blockSize;
    private boolean _outputEmpty = false;
    private boolean _tRewrite = true;
    private MatrixBlock blkOut;
    private JavaPairRDD<MatrixIndexes, MatrixBlock> out;
    private String outputIn1Path = null;
    private String outputIn2Path = null;

    public void setMm(MatrixMultiply mm) {
        this.mm = mm;
    }

    public boolean is_outputEmpty() {
        return _outputEmpty;
    }

    public void set_outputEmpty(boolean _outputEmpty) {
        this._outputEmpty = _outputEmpty;
    }

    public boolean is_tRewrite() {
        return _tRewrite;
    }

    public void set_tRewrite(boolean _tRewrite) {
        this._tRewrite = _tRewrite;
    }

    public MatrixBlock getBlkOut() {
        return blkOut;
    }

    public void setOutputIn1Path(String outputIn1Path) {
        this.outputIn1Path = outputIn1Path;
    }

    public void setOutputIn2Path(String outputIn2Path) {
        this.outputIn2Path = outputIn2Path;
    }

    public void set_cacheType(CacheTpye _cacheType) {
        this._cacheType = _cacheType;
    }

    public void set_aggType(SparkAggType _aggType) {
        this._aggType = _aggType;
    }

    public JavaPairRDD<MatrixIndexes, MatrixBlock> getOut() {
        return out;
    }

    public void execute() throws Exception {
        switch (_methodType) {
            case CpMM:
//                out = CpMM.execute(in1, in2, mc1, mc2);
                mm = new CpMM();
                out = mm.execute(in1, in2, mc1, mc2);
                break;
            case PMapMM:
                out = PMapMM.execute(sc, in1, in2, mc1);
                break;
            case RMM:
//                out = RMM.execute(in1, in2, mc1, mc2);
                mm = new RMM();
                out = mm.execute(in1, in2, mc1, mc2);
                break;
            case ZipMM:
                blkOut = ZipMM.execute(in1, in2, _tRewrite);
                break;
            case MapMM:
                if (_cacheType == null || _aggType == null)
                    throw new Exception("please set _cacheType and _aggType");
//                blkOut = MapMM.execute(sc, in1, in2, mc1, mc2, _blockSize, _cacheType, _aggType, _outputEmpty);
                MapMM mapMM = new MapMM();
                mapMM.setOutputEmpty(_outputEmpty);
                mapMM.setType(_cacheType);
                mapMM.set_aggType(_aggType);
                mapMM.setSc(sc);
                mapMM.setBlen(_blockSize);
                mm = mapMM;
                out = mm.execute(in1, in2, mc1, mc2);
                break;
            case CRMM:
                out = CRMM.execute(in1, in2, mc1, mc2);
                break;
            case TEST:
                if (outputIn1Path == null && outputIn2Path == null) {
                    in1.collect();
                    in2.collect();
                    System.out.println("you can run here");
                    return;
                }
                IOUtil.saveMatrixAsCSVFile(in1, outputIn1Path, mc1);
                if (outputIn2Path != null)
                    IOUtil.saveMatrixAsCSVFile(in2, outputIn2Path, mc2);
                break;
            default:
                throw new Exception("have not supported this method!");
        }
    }


}
