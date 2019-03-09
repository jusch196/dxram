package de.hhu.bsinfo.dxram.ms.tasks.mergesortapplication;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
 * Task to Sort data localy (sortedData.csv)
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */
public class SortTask implements Task {

    private static ChunkService chunkService;
    private final static int GLOBAL_CHUNK_SIZE = 64;

    private static boolean powTwo = true;
    private static int partialListLength[];
    private static long[] chunkAddress;
    private static Thread[] threads;

    public SortTask() {
    }

    @Override
    public int execute(TaskContext p_ctx) throws IOException {
        System.out.println("Start: " + System.nanoTime());

        chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

        short ownSlaveID = p_ctx.getCtxData().getSlaveId();

        // Get SortTaskBeginChunk
        int size = getIntData(nameService.getChunkID("SAC" + ownSlaveID, 1000));
        chunkAddress = getLongArray(nameService.getChunkID("AC" + ownSlaveID, 1000), size);

        int availableResources = Runtime.getRuntime().availableProcessors();

        threads = new Thread[availableResources];
        partialListLength = new int[availableResources];
        int lengthOfSplits = chunkAddress.length/availableResources;
        int overhead = chunkAddress.length % availableResources;

        // Run John von Neumann mergesort on each partial list
        for (int i = 0, j = 0; i < availableResources; i++) {
            if (j < overhead) {
                threads[i] = new SortAlgorithm(chunkAddress, i * lengthOfSplits + j, lengthOfSplits+1, chunkService);
                partialListLength[i] = lengthOfSplits + 1;
                j++;
            } else {
                threads[i] = new SortAlgorithm(chunkAddress, (i * lengthOfSplits) + j, lengthOfSplits, chunkService);
                partialListLength[i] = lengthOfSplits;
            }
        }

        while (availableResources > 1){
            double splitCheck = (double) availableResources/2;
            if (splitCheck %1 != 0){
                powTwo = false;
            }

            availableResources /= 2;
            threads = new Thread[availableResources];

            for (int i = 0; i < availableResources; i++)
                merge(i);

            // Update listlength
            int[] tmp;
            if (!powTwo){
                tmp = new int[availableResources+1];

                for (int i = 0; i < tmp.length-1; i++) {
                    tmp[i] = partialListLength[2 * i];
                    if (2 * i + 1 < partialListLength.length) {
                        tmp[i] += partialListLength[2 * i + 1];
                    }
                }

                tmp[tmp.length-1] = partialListLength[partialListLength.length-1];
                powTwo = true;
                availableResources++;
            } else{
                tmp = new int[availableResources];

                for (int i = 0; i < tmp.length; i++) {
                    tmp[i] = partialListLength[2 * i];
                    if (2 * i + 1 < partialListLength.length) {
                        tmp[i] += partialListLength[2 * i + 1];
                    }
                }
            }
            partialListLength = tmp;
        }

        // Update Chunkaddresses
        editChunkLongArray(chunkAddress, nameService.getChunkID("AC" + ownSlaveID, 100), chunkService);

        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {
        // Doesn't handle signals
    }

    @Override
    public void exportObject(Exporter p_exporter) {

    }

    @Override
    public void importObject(Importer p_importer) {

    }

    @Override
    public int sizeofObject() {
        return 0;
    }

    /**
     * Edits the longarray of a chunk
     *
     * @param array
     *          longarray to put
     * @param chunkId
     *          ChunkID of the editable chunk
     * @param chunkService
     *          Chunkservice to manage the operation
     */
    private void editChunkLongArray(long[] array, long chunkId, ChunkService chunkService){
        ByteBuffer byteBuffer = ByteBuffer.allocate(array.length*GLOBAL_CHUNK_SIZE);
        LongBuffer longBuffer = byteBuffer.asLongBuffer();
        longBuffer.put(array);
        ChunkByteArray chunkByteArray = new ChunkByteArray(chunkId, byteBuffer.array());
        chunkService.put().put(chunkByteArray);
    }

    /**
     * Returns a longarray stored in DXRAM
     * @param chunkId
     *          ID of the chunk where the array is stored
     * @param size
     *          Size of the array
     * @return
     *      Returns the stored array
     */
    private long[] getLongArray(long chunkId, int size) {
        ChunkByteArray testChunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE*size);
        chunkService.get().get(testChunk);
        byte[] byteData = testChunk.getData();
        LongBuffer longBuffer = ByteBuffer.wrap(byteData)
                .order(ByteOrder.BIG_ENDIAN)
                .asLongBuffer();

        long[] longArray = new long[size];
        longBuffer.get(longArray);

        return longArray;

    }

    /**
     * Get the integervalue of a chunk
     * @param chunkId
     *          ID of the chunk
     * @return
     *      Integervalue of the chunk
     */
    private int getIntData(long chunkId ){
        ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
        chunkService.get().get(chunk);
        byte[] byteData = chunk.getData();

        return ByteBuffer.wrap(byteData).getInt();
    }

    /**
     * Merges two half's of a block which indices are stored in listlength*
     * @param blockIndex
     *          Index of the block to sort
     */
    private static void merge(int blockIndex ){

        int start=0, breakpoint, end;

        for (int i=0; i<2*blockIndex;i++){
            start += partialListLength[i];
        }

        breakpoint = start + partialListLength[2*blockIndex];
        end = breakpoint + partialListLength[2*blockIndex+1] -1;

        threads[blockIndex] = new MergeAlgorithm(chunkAddress,start,end,breakpoint, chunkService);
    }
}
