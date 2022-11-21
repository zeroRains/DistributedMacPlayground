package com.distributedMacPlayground.util;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.controlprogram.caching.CacheBlock;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.instructions.spark.data.BroadcastObject;
import org.apache.sysds.runtime.instructions.spark.data.PartitionedBlock;
import org.apache.sysds.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

import java.util.Arrays;

public class ExecutionUtil {
    /**
     * make a MatrixObject to broadcast in sparkContext.
     *
     * @param sc sparkcontext
     * @param mo matrixObject
     * @return
     */
    public static PartitionedBroadcast<MatrixBlock> broadcastForMatrixObject(JavaSparkContext sc, MatrixObject mo) {
        PartitionedBroadcast<MatrixBlock> bret = null;
        synchronized (mo) {
            if (mo.getBroadcastHandle() != null && mo.getBroadcastHandle().isPartitionedBroadcastValid()) {
                bret = mo.getBroadcastHandle().getPartitionedBroadcast();
            }

            if (bret == null) {
                if (mo.getBroadcastHandle() != null)
                    CacheableData.addBroadcastSize(-mo.getBroadcastHandle().getSize());
                int blen = (int) mo.getBlocksize();

                MatrixBlock mb = mo.acquireRead();
                PartitionedBlock<MatrixBlock> pmb = new PartitionedBlock<>(mb, blen);
                mo.release();

                int numPerPart = PartitionedBroadcast.computeBlocksPerPartition(mo.getNumRows(), mo.getNumColumns(), blen);
                int numParts = (int) Math.ceil((double) pmb.getNumRowBlocks() * pmb.getNumColumnBlocks() / numPerPart);
                Broadcast<PartitionedBlock<MatrixBlock>>[] ret = new Broadcast[numParts];

                if (numParts > 1) {
                    Arrays.parallelSetAll(ret, i -> createPartitionedBroadcast(sc, pmb, numPerPart, i));
                } else {
                    ret[0] = sc.broadcast(pmb);
                    if (sc.isLocal())
                        pmb.clearBlocks();
                }
                bret = new PartitionedBroadcast<>(ret, mo.getDataCharacteristics());

                if (mo.getBroadcastHandle() == null) {
                    mo.setBroadcastHandle(new BroadcastObject<MatrixBlock>());
                }

                mo.getBroadcastHandle().setPartitionedBroadcast(bret,
                        OptimizerUtils.estimatePartitionedSizeExactSparsity(mo.getDataCharacteristics()));
                CacheableData.addBroadcastSize(mo.getBroadcastHandle().getSize());
            }
        }
        return bret;
    }

    /**
     * Broadcast a partition
     *
     * @param sc         sparkContext
     * @param pmb        partionedBlock
     * @param numPerPart number of per partition
     * @param pos        position
     * @return
     */
    private static Broadcast<PartitionedBlock<MatrixBlock>> createPartitionedBroadcast(
            JavaSparkContext sc, PartitionedBlock<MatrixBlock> pmb, int numPerPart, int pos) {
        int offset = pos * numPerPart;
        int numBlks = Math.min(numPerPart, pmb.getNumColumnBlocks() * pmb.getNumColumnBlocks() - offset);
        PartitionedBlock<MatrixBlock> tmp = pmb.createPartition(offset, numBlks);
        Broadcast<PartitionedBlock<MatrixBlock>> ret = sc.broadcast(tmp);
        if (!sc.isLocal())
            tmp.clearBlocks();
        return ret;
    }


}
