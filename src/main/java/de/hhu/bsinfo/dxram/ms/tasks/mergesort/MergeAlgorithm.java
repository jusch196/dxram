package de.hhu.bsinfo.dxram.ms.tasks.mergesort;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import java.nio.ByteBuffer;

/**
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */

class MergeAlgorithm extends Thread {

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

        long[] finalArray = new long[end-start+1];

        int indexLeft = start;
        int indexRight = breakpoint;
        int finalIndex = 0;

        while (indexLeft < breakpoint && indexRight <= end) {

            if (getIntData(array[indexLeft], 64, chunkService) < getIntData(array[indexRight], 64, chunkService)) {
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
     * @param chunkService
     *          Chunkservice to manage the operation
     * @return
     *      Integervalue of the chunk
     */
    private static int getIntData(long chunkId, int size, ChunkService chunkService){
        ChunkByteArray chunk = new ChunkByteArray(chunkId, size);
        chunkService.get().get(chunk);
        byte[] byteData = chunk.getData();
        return ByteBuffer.wrap(byteData).getInt();
    }
}

