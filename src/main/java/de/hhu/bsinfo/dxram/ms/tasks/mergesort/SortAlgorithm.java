package de.hhu.bsinfo.dxram.ms.tasks.mergesort;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.nio.ByteBuffer;

class SortAlgorithm extends Thread {

        private final static int GLOBAL_CHUNK_SIZE = 64;

        private static long[] array;
        private static ChunkService chunkService;

        SortAlgorithm(long[] array, int start, int length, ChunkService chunkService){

                //this.array = array;
                SortAlgorithm.array = array;
                SortAlgorithm.chunkService = chunkService;
                try {
                        join();
                } catch (InterruptedException e) {
                        e.printStackTrace();
                }
                sort(start, length);

        }

        private static void merge(int start, int length, int breakpoint) {

                long[] finalArray = new long[length];

                int indexLeft = 0;
                int indexRight = 0;
                int finalIndex = 0;

                while (indexLeft < breakpoint && indexRight < length-breakpoint) {

                        if (getIntData(array[start+indexLeft], chunkService) < getIntData(array[start + breakpoint + indexRight], chunkService)) {
                                finalArray[finalIndex] = array[start + indexLeft];
                                indexLeft++;
                        } else {
                                finalArray[finalIndex] = array[start + breakpoint + indexRight];
                                indexRight++;
                        }
                        finalIndex++;
                }

                while (indexLeft < breakpoint) {
                        finalArray[finalIndex] = array[start + indexLeft];
                        indexLeft++;
                        finalIndex++;
                }

                while (indexRight < length-breakpoint) {
                        finalArray[finalIndex] = array[start +breakpoint + indexRight];
                        indexRight++;
                        finalIndex++;
                }

                System.arraycopy(finalArray, 0, array, start, finalIndex);
        }

        private static void sort(int start, int length){

                if ( length > 1) {

                        sort(start, (length/2));
                        sort(start+(length/2), (length-(length/2)));

                        merge(start, length, length/2);
                }

        }

        private static int getIntData(long chunkId, ChunkService chunkService){
                ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
                chunkService.get().get(chunk);
                byte[] byteData = chunk.getData();
                return ByteBuffer.wrap(byteData).getInt();
        }
}
