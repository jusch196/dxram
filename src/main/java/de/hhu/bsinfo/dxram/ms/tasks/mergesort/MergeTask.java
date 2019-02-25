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

public class MergeTask implements Task {

    private final static int GLOBAL_CHUNK_SIZE = 64;
    private static ChunkService chunkService;

    public MergeTask() {
    }

    @Override
    public int execute(TaskContext p_ctx) {

        // Get Services
        NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
        chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);

        short ownSlaveID = p_ctx.getCtxData().getSlaveId();
        int ownIndex = Short.toUnsignedInt(ownSlaveID);
        int goThrough = getIntData(nameService.getChunkID("GT" + ownIndex, 1000));

        int numberOfWorkingNodes = p_ctx.getCtxData().getSlaveNodeIds().length;

        System.out.println("Starte merge-Task");

        if (ownIndex % goThrough == 0){

            int checkIfLastUneven = (2*numberOfWorkingNodes)/goThrough % 2;

            if (checkIfLastUneven != 1 || ownIndex != checkIfLastUneven-1){

                if (numberOfWorkingNodes % 2 == 0) {

                    int test = ownIndex +1;

                    int sizeone = getIntData(nameService.getChunkID("SAC" + ownIndex, 1000));
                    int sizetwo = getIntData(nameService.getChunkID("SAC" + test, 1000));

                    long[] firstChunkAdresses = getLongArray(nameService.getChunkID("AC" + ownIndex, 1000), sizeone);
                    long[] secondChunkAdresses = getLongArray(nameService.getChunkID("AC" + test, 1000), sizetwo);

                    long[] finalArray = new long[sizeone+sizetwo];
                    int indexLeft = 0;
                    int indexRight = 0;
                    int finalIndex = 0;

                    while (indexLeft < sizeone && indexRight < sizetwo) {

                        if (getIntData(firstChunkAdresses[indexLeft]) < getIntData(secondChunkAdresses[indexRight])) {
                            finalArray[finalIndex] = firstChunkAdresses[indexLeft];
                            indexLeft++;
                        } else {
                            finalArray[finalIndex] = secondChunkAdresses[indexRight];
                            indexRight++;
                        }
                        finalIndex++;
                    }

                    while (indexLeft < sizeone) {
                        finalArray[finalIndex] = firstChunkAdresses[indexLeft];
                        indexLeft++;
                        finalIndex++;
                    }

                    while (indexRight < sizetwo) {
                        finalArray[finalIndex] = secondChunkAdresses[indexRight];
                        indexRight++;
                        finalIndex++;
                    }

                    // Remove old Adresschunks
                    chunkService.remove().remove(nameService.getChunkID("AC" + ownIndex, 1000));
                    chunkService.remove().remove(nameService.getChunkID("AC" + ownIndex+1, 1000));
                    chunkService.remove().remove(nameService.getChunkID("SAC" + ownIndex, 1000));
                    chunkService.remove().remove(nameService.getChunkID("SAC" + ownIndex+1, 1000));


                    // Update Addresses
                    long[] tmpAddressChunkId = new long[1];
                    chunkService.create().create(p_ctx.getCtxData().getOwnNodeId(), tmpAddressChunkId, 1, GLOBAL_CHUNK_SIZE*finalArray.length);
                    editChunkArray(finalArray, tmpAddressChunkId[0], chunkService);
                    nameService.register(tmpAddressChunkId[0], "AC" + ownIndex/2);

                    // Update Size
                    chunkService.create().create(p_ctx.getCtxData().getOwnNodeId(), tmpAddressChunkId, 1, GLOBAL_CHUNK_SIZE);
                    editChunkInt(finalArray.length, tmpAddressChunkId[0], chunkService);
                    nameService.register(tmpAddressChunkId[0], "SAC" + ownIndex/2);

                    // Update goThrough
                    editChunkInt(goThrough*2, nameService.getChunkID("GT", 1000), chunkService);
                }
            }
        } else {
            return 0;
        }




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

    private void editChunkArray (long[] array, long chunkId, ChunkService chunkService){
        ByteBuffer byteBuffer = ByteBuffer.allocate(array.length*GLOBAL_CHUNK_SIZE);
        LongBuffer longBuffer = byteBuffer.asLongBuffer();
        longBuffer.put(array);
        ChunkByteArray chunkByteArray = new ChunkByteArray(chunkId, byteBuffer.array());
        chunkService.put().put(chunkByteArray);
    }

    private void editChunkInt(int value, long chunkId , ChunkService chunkService){
        ByteBuffer byteBuffer = ByteBuffer.allocate(GLOBAL_CHUNK_SIZE);
        byteBuffer.putInt(value);
        ChunkByteArray chunkByteArray = new ChunkByteArray(chunkId, byteBuffer.array());
        chunkService.put().put(chunkByteArray);
    }
}
