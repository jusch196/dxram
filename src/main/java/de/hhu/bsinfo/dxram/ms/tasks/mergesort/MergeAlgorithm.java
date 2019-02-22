package de.hhu.bsinfo.dxram.ms.tasks.mergesort;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.nio.ByteBuffer;

class MergeAlgorithm extends Thread {

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

    private static int getIntData(long chunkId, int size, ChunkService chunkService){
        ChunkByteArray chunk = new ChunkByteArray(chunkId, size);
        chunkService.get().get(chunk);
        byte[] byteData = chunk.getData();
        return ByteBuffer.wrap(byteData).getInt();
    }
}

