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

/**
 * Task to merge the presorted data (sortedData.csv) in case of uneven number
 * of workernodes
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */

public class UnevenMergeTask implements Task {

    private final static int GLOBAL_CHUNK_SIZE = 64;
    private static ChunkService chunkService;

    public UnevenMergeTask() {
    }


    @Override
    public int execute(TaskContext p_ctx) throws IOException {
        NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
        chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);

        short ownSlaveID = p_ctx.getCtxData().getSlaveId();
        int ownIndex = Short.toUnsignedInt(ownSlaveID);
        int goThrough = getIntData(nameService.getChunkID("GT", 1000));

        if (ownSlaveID == p_ctx.getCtxData().getSlaveNodeIds().length -1 && ownIndex %2 == 0){
                nameService.register(nameService.getChunkID("AC" +ownIndex, 100), "AC" + ownIndex/2);
                nameService.register(nameService.getChunkID("SAC" +ownIndex, 100), "SAC" + ownIndex/2);

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
