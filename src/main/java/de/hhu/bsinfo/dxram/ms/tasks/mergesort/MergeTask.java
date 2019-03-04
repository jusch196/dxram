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

/**
 * Task to merge the presorted data (sortedData.csv)
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */
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

        if (ownIndex % goThrough == 1){

            int partnerIndex = ownIndex - 1;

            int sizeone = getIntData(nameService.getChunkID("SAC" + partnerIndex, 1000));
            int sizetwo = getIntData(nameService.getChunkID("SAC" + ownIndex, 1000));

            long[] firstChunkAdresses = getLongArray(nameService.getChunkID("AC" + partnerIndex, 1000), sizeone);
            long[] secondChunkAdresses = getLongArray(nameService.getChunkID("AC" + ownIndex, 1000), sizetwo);

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
            System.out.println("BEENDET");
            // Remove old Adresschunks
            chunkService.remove().remove(nameService.getChunkID("AC" + partnerIndex, 1000));
            chunkService.remove().remove(nameService.getChunkID("AC" + ownIndex, 1000));
            chunkService.remove().remove(nameService.getChunkID("SAC" + partnerIndex, 1000));
            chunkService.remove().remove(nameService.getChunkID("SAC" + ownIndex, 1000));


            // Update Addresses
            long[] tmpAddressChunkId = new long[1];
            chunkService.create().create(p_ctx.getCtxData().getOwnNodeId(), tmpAddressChunkId, 1, GLOBAL_CHUNK_SIZE*finalArray.length);
            editChunkLongArray(finalArray, tmpAddressChunkId[0], chunkService);
            nameService.register(tmpAddressChunkId[0], "AC" + partnerIndex/2);

            // Update Size
            chunkService.create().create(p_ctx.getCtxData().getOwnNodeId(), tmpAddressChunkId, 1, GLOBAL_CHUNK_SIZE);
            editChunkInt(finalArray.length, tmpAddressChunkId[0], chunkService);
            nameService.register(tmpAddressChunkId[0], "SAC" + partnerIndex/2);

            // Update goThrough
            editChunkInt(goThrough*2, nameService.getChunkID("GT" +partnerIndex, 1000), chunkService);
            System.out.println("BEENDET");
        } else
            return 0;

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
     * Gets a long array stored in a chunk
     *
     * @param chunkId
     *          ID of the chunk
     * @param size
     *          Size of the array
     * @return
     *          Returns a long[]-array containing the values of the chunk
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
     * Edits the integervalue of a chunk
     *
     * @param value
     *          Integervalue to put
     * @param chunkId
     *          ChunkID of the editable chunk
     * @param chunkService
     *          Chunkservice to manage the operation
     */
    private void editChunkInt(int value, long chunkId , ChunkService chunkService){
        ByteBuffer byteBuffer = ByteBuffer.allocate(GLOBAL_CHUNK_SIZE);
        byteBuffer.putInt(value);
        ChunkByteArray chunkByteArray = new ChunkByteArray(chunkId, byteBuffer.array());
        chunkService.put().put(chunkByteArray);
    }
}
