package de.hhu.bsinfo.dxram.ms.tasks.mergesortapplication;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import java.nio.ByteBuffer;

/**
 * Sorts the array local on one thread based on John von Neumann mergesort
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
                mergeSortJvN(array,start, length);

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

                int first=0, second=0;
                boolean run = false;

                if (indexLeft < breakpoint && indexRight < length-breakpoint){
                        first = getIntData(array[start+indexLeft]);
                        second = getIntData(array[start + breakpoint + indexRight]);
                        run = true;
                }

                while (run) {
                        if (first < second) {
                                finalArray[finalIndex] = array[start + indexLeft];
                                indexLeft++;
                                if (indexLeft < breakpoint)
                                        first = getIntData(array[start+indexLeft]);
                                else
                                        run = false;
                        } else {
                                finalArray[finalIndex] = array[start + breakpoint + indexRight];
                                indexRight++;
                                if (indexRight < length-breakpoint)
                                        second = getIntData(array[start + breakpoint + indexRight]);
                                else
                                        run = false;
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
         * @return
         *      Integervalue of the chunk
         */
        private static int getIntData(long chunkId){
                ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
                chunkService.get().get(chunk);
                byte[] byteData = chunk.getData();
                return ByteBuffer.wrap(byteData).getInt();
        }

    private static void mergeJvN(long[] array, int left, int breakpoint, int right) {
        int i, j, k;
        int n1 = breakpoint - left + 1;
        int n2 =  right - breakpoint;

        // create temp arrays
        long[] L = new long[n1];
        long[] R = new long[n2];

        // Copy data to temp arrays L[] and R[]
        for (i = 0; i < n1; i++)
            L[i] = array[left + i];
        for (j = 0; j < n2; j++)
            R[j] = array[breakpoint + 1+ j];

        // Merge the temp arrays back into arr[l..r]
        i = 0; // Initial index of first subarray
        j = 0; // Initial index of second subarray
        k = left; // Initial index of merged subarray

        int first=0, second=0;
        boolean run = false;

        if (i < n1 && j < n2){
            first = getIntData(L[i]);
            second = getIntData(R[j]);
            run = true;
        }

        while (run)
        {
            if (first <= second) {
                array[k] = L[i];
                i++;
                if (i<n1)
                    first = getIntData(L[i]);
                else
                    run=false;
            }
            else {
                array[k] = R[j];
                j++;
                if (j<n2)
                    second=getIntData(R[j]);
                else
                    run=false;
            }
            k++;
        }

        //Copy the remaining elements of L[], if there are any
        while (i < n1)
        {
            array[k] = L[i];
            i++;
            k++;
        }

        // Copy the remaining elements of R[], if there are any
        while (j < n2)
        {
            array[k] = R[j];
            j++;
            k++;
        }
    }
    private static void mergeSortJvN(long[] array, int left, int right) {
        if (left < right)
        {
            // Same as (l+r)/2, but avoids overflow for
            // large l and h
            int m = left+(right-left)/2;

            // Sort first and second halves
            mergeSortJvN(array, left, m);
            mergeSortJvN(array, m+1, right);

            mergeJvN(array, left, m, right);
        }
    }
}
