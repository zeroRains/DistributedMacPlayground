package com.distributedMacPlayground;

import com.distributedMacPlayground.CommonConfig.MMMethodType;
import com.distributedMacPlayground.CommonConfig.CacheTpye;
import com.distributedMacPlayground.CommonConfig.SparkAggType;
import com.distributedMacPlayground.method.*;
import com.distributedMacPlayground.util.ExecutionUtil;
import com.distributedMacPlayground.util.IOUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

    // get data from csv file (without header), expect MapMM
    public RunMethod(JavaSparkContext sc, MMMethodType _methodType, int _outRowLen, int _outColLen, int _middleLen, String matrixPath1, String matrixPath2) {
        this.sc = sc;
        this._methodType = _methodType;
        this._outRowLen = _outRowLen;
        this._outColLen = _outColLen;
        this._middleLen = _middleLen;
        this.matrixPath1 = matrixPath1;
        this.matrixPath2 = matrixPath2;
        this.mc1 = new MatrixCharacteristics(_outRowLen, _middleLen, _blockSize);
        this.mc2 = new MatrixCharacteristics(_middleLen, _outColLen, _blockSize);
        in1 = IOUtil.csvFileToMatrixRDD(this.sc, this.matrixPath1, this.mc1);
        in2 = IOUtil.csvFileToMatrixRDD(this.sc, this.matrixPath2, this.mc2);
    }

    // get data from MatrixBlock, expect MapMM
    public RunMethod(JavaSparkContext sc, MMMethodType _methodType, int _outRowLen, int _outColLen, int _middleLen, MatrixBlock blkIn1, MatrixBlock blkIn2) {
        this.sc = sc;
        this._methodType = _methodType;
        this._outRowLen = _outRowLen;
        this._outColLen = _outColLen;
        this._middleLen = _middleLen;
        this.blkIn1 = blkIn1;
        this.blkIn2 = blkIn2;
        this.mc1 = new MatrixCharacteristics(_outRowLen, _middleLen, _blockSize);
        this.mc2 = new MatrixCharacteristics(_middleLen, _outColLen, _blockSize);
    }

    // get data from csv file (without header), use for MapMM
    public RunMethod(JavaSparkContext sc, MMMethodType _methodType, int _outRowLen, int _outColLen, int _middleLen, CacheTpye _cacheType, SparkAggType _aggType, String matrixPath1, String matrixPath2) {
        this.sc = sc;
        this._methodType = _methodType;
        this._outRowLen = _outRowLen;
        this._outColLen = _outColLen;
        this._middleLen = _middleLen;
        this._cacheType = _cacheType;
        this._aggType = _aggType;
        this.matrixPath1 = matrixPath1;
        this.matrixPath2 = matrixPath2;
        this.mc1 = new MatrixCharacteristics(_outRowLen, _middleLen, _blockSize);
        this.mc2 = new MatrixCharacteristics(_middleLen, _outColLen, _blockSize);
        in1 = IOUtil.csvFileToMatrixRDD(this.sc, this.matrixPath1, this.mc1);
        in2 = IOUtil.csvFileToMatrixRDD(this.sc, this.matrixPath2, this.mc2);
        blkIn1 = ExecutionUtil.matrixRDDToMatrixBlock(this.in1, this._outRowLen, this._middleLen, _blockSize);
        blkIn2 = ExecutionUtil.matrixRDDToMatrixBlock(this.in2, this._middleLen, this._outColLen, _blockSize);
    }

    // get data from MatrixBlock, use for MapMM
    public RunMethod(JavaSparkContext sc, MMMethodType _methodType, int _outRowLen, int _outColLen, int _middleLen, CacheTpye _cacheType, SparkAggType _aggType, MatrixBlock blkIn1, MatrixBlock blkIn2) {
        this.sc = sc;
        this._methodType = _methodType;
        this._outRowLen = _outRowLen;
        this._outColLen = _outColLen;
        this._middleLen = _middleLen;
        this._cacheType = _cacheType;
        this._aggType = _aggType;
        this.blkIn1 = blkIn1;
        this.blkIn2 = blkIn2;
    }

    private int _blockSize = 1000;
    private int _numParts = -1;
    private boolean _outputEmpty = false;
    private boolean _tRewrite = true;
    private MatrixBlock blkOut;
    private JavaPairRDD<MatrixIndexes, MatrixBlock> out;

    public int get_blockSize() {
        return _blockSize;
    }

    public void set_blockSize(int _blockSize) {
        this._blockSize = _blockSize;
    }

    public int get_numParts() {
        return _numParts;
    }

    public void set_numParts(int _numParts) {
        this._numParts = _numParts;
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

    public JavaPairRDD<MatrixIndexes, MatrixBlock> getOut() {
        return out;
    }

    public void execute() throws Exception {
        switch (_methodType) {
            case CpMM:
                out = CpMM.execute(in1, in2, mc1, mc2);
                break;
            case PMapMM:
                out = PMapMM.execute(sc, in1, in2, mc1);
                break;
            case RMM:
                out = RMM.execute(in1, in2, mc1, mc2);
                break;
            case ZipMM:
                blkOut = ZipMM.execute(in1, in2, _tRewrite);
            case MapMM:
                if (_cacheType == null || _aggType == null)
                    throw new Exception("please set _cacheType and _aggType");
                blkOut = MapMM.execute(sc, blkIn1, blkIn2, _blockSize, _cacheType, _aggType);
                break;
            default:
                throw new Exception("have not supported this method!");
        }
    }


}
