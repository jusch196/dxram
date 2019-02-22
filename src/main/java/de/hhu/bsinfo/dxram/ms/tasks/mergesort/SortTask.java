package de.hhu.bsinfo.dxram.ms.tasks.mergesort;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;

public class SortTask implements Task {

    private static ChunkService chunkService;
    private final static int GLOBAL_CHUNK_SIZE = 64;

    private static boolean powtwo = true;
    private static int partialListLength[];
    private static long[] chunkAdress;
    private static Thread[] threads;

    public SortTask() {
    }

    @Override
    public int execute(TaskContext p_ctx) {

        System.out.println("Starte Sort-Task");

        chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

        short ownSlaveID = p_ctx.getCtxData().getSlaveId();
        int ownIndex = Short.toUnsignedInt(ownSlaveID);

        // Get SortTaskBeginChunk
        int size = getIntData(nameService.getChunkID("SAC" + ownIndex, 1000));
        chunkAdress = getLongArray(nameService.getChunkID("AC" + ownIndex, 1000), size);


        int availableResources = Runtime.getRuntime().availableProcessors();

        threads = new Thread[availableResources];
        partialListLength = new int[availableResources];
        int lengthOfSplits = chunkAdress.length/availableResources;
        int overhead = chunkAdress.length % availableResources;

        // Run normal mergesort
        for (int i = 0, j = 0; i < availableResources; i++) {
            if (j < overhead) {
                threads[i] = new SortAlgorithm(chunkAdress, i * lengthOfSplits + j, lengthOfSplits + 1, chunkService);
                partialListLength[i] = lengthOfSplits + 1;
                j++;
            } else {
                threads[i] = new SortAlgorithm(chunkAdress, (i * lengthOfSplits) + overhead, lengthOfSplits, chunkService);
                partialListLength[i] = lengthOfSplits;
            }
        }

        while (availableResources > 1){
            double splitcheck = (double) availableResources/2;
            if (splitcheck %1 != 0){
                powtwo = false;
            }

            availableResources /= 2;
            threads = new Thread[availableResources];

            //  i represents one block (left AND right half)
            for (int i = 0; i < availableResources; i++) {
                merge(i);
            }

            // Update listlength
            int[] tmp;
            if (!powtwo){
                tmp = new int[availableResources+1];

                for (int i = 0; i < tmp.length-1; i++) {
                    tmp[i] = partialListLength[2 * i];
                    if (2 * i + 1 < partialListLength.length) {
                        tmp[i] += partialListLength[2 * i + 1];
                    }
                }
                tmp[tmp.length-1] = partialListLength[partialListLength.length-1];
                powtwo = true;
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

        int[] chunkValues = new int[chunkAdress.length];
        for (int i=0;i<chunkAdress.length; i++){
            chunkValues[i] = getIntData(chunkAdress[i]);
        }
        System.out.println(Arrays.toString(chunkValues));

        // Update Addresses
        editChunkArray(chunkAdress, nameService.getChunkID("AC" + ownIndex, 1000), chunkService);

        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {

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


    private void editChunkArray (long[] array, long chunkId, ChunkService chunkService){
        ByteBuffer byteBuffer = ByteBuffer.allocate(array.length*GLOBAL_CHUNK_SIZE);
        LongBuffer longBuffer = byteBuffer.asLongBuffer();
        longBuffer.put(array);
        ChunkByteArray chunkByteArray = new ChunkByteArray(chunkId, byteBuffer.array());
        chunkService.put().put(chunkByteArray);
    }

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

    private int getIntData(long chunkId ){
        ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
        chunkService.get().get(chunk);
        byte[] byteData = chunk.getData();
        return ByteBuffer.wrap(byteData).getInt();
    }

    private static void merge(int blockIndex ){

        int start=0, breakpoint, end;

        for (int i=0; i<2*blockIndex;i++){
            start += partialListLength[i];
        }
        breakpoint = start + partialListLength[2*blockIndex];
        end = breakpoint + partialListLength[2*blockIndex+1] -1;

        //System.out.println(Arrays.toString(partialListLength));
        threads[blockIndex] = new MergeAlgorithm(chunkAdress,start,end,breakpoint, chunkService);

    }
}
