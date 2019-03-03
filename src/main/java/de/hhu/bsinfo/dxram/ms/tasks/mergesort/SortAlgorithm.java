package de.hhu.bsinfo.dxram.ms.tasks.mergesort;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import java.nio.ByteBuffer;

/**
 * Sorts the array based on John von Neumann mergesort
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */

class SortAlgorithm extends Thread {

        private final static int GLOBAL_CHUNK_SIZE = 64;

        private static long[] array;
        private static ChunkService chunkService;

        SortAlgorithm(long[] array, int start, int length, ChunkService chunkService){

                SortAlgorithm.array = array;
                SortAlgorithm.chunkService = chunkService;
                try {
                        join();
                } catch (InterruptedException e) {
                        e.printStackTrace();
                }
                sort(start, length);

        }

        /**
         * Merges two halfs stored next to each other in array
         * based on John von Neumann mergesort
         *
         * @param start
         *              Startindex of the partial list
         * @param length
         *              Length of the two half's
         * @param breakpoint
         *              First index of the right half two separate both halfs
         */
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

        /**
         * Sorts a partial list stored in the array
         * based on John von Neumann mergesort
         *
         * @param start
         *              Startindex of the partial list
         * @param endIndex
         *              Endindex of the partial list
         */
        private static void sort(int start, int endIndex){
                if ( endIndex > 1) {
                        sort(start, (endIndex/2));
                        sort(start+(endIndex/2), (endIndex-(endIndex/2)));
                        merge(start, endIndex, endIndex/2);
                }
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
        private static int getIntData(long chunkId, ChunkService chunkService){
                ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
                chunkService.get().get(chunk);
                byte[] byteData = chunk.getData();
                return ByteBuffer.wrap(byteData).getInt();
        }
}
