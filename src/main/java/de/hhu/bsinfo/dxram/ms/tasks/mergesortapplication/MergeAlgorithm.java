package de.hhu.bsinfo.dxram.ms.tasks.mergesortapplication;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import java.nio.ByteBuffer;

/**
 * Merges the adresseslists of the data local on one thread
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */
class MergeAlgorithm extends Thread {

    private final static int GLOBAL_CHUNK_SIZE = 64;
    private static ChunkService chunkService;

    /**
     * Merges the addresses of two partial lists
     * stored next to each other comparing their values
     * based on John von Neumann mergesortalgorithm
     *
     * @param start
     *          Startindex of the partial list
     * @param end
     *          Endindex of the partial list
     * @param breakpoint
     *          Breakpointindex to separate the two half's
     * @param chunkService
     *          Chunkservice to manage the operations
     */
    MergeAlgorithm(long[] array, int start, int end, int breakpoint, ChunkService chunkService){

        MergeAlgorithm.chunkService = chunkService;
        long[] finalArray = new long[end-start+1];

        int indexLeft = start;
        int indexRight = breakpoint;
        int finalIndex = 0;

        while (indexLeft < breakpoint && indexRight <= end) {

            if (getIntData(array[indexLeft]) < getIntData(array[indexRight])) {
                finalArray[finalIndex] = array[indexLeft];
                indexLeft++;
            } else {
                finalArray[finalIndex] = array[indexRight];
                indexRight++;
            }
            finalIndex++;
        }

        while (indexLeft < breakpoint) {
            finalArray[finalIndex] = array[indexLeft];
            indexLeft++;
            finalIndex++;
        }

        while (indexRight <= end) {
            finalArray[finalIndex] = array[indexRight];
            indexRight++;
            finalIndex++;
        }

        System.arraycopy(finalArray, 0, array, start, finalIndex);
    }

    /**
     * Get the integervalue of a chunk
     * @param chunkId
     *          ID of the chunk
     * @return
     *      Integervalue of the chunk
     */
    private static int getIntData(long chunkId){
        ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
        chunkService.get().get(chunk);
        byte[] byteData = chunk.getData();

        return ByteBuffer.wrap(byteData).getInt();
    }
}

