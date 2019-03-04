package de.hhu.bsinfo.dxram.ms.tasks.mergesortapplication;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
 * Task to export the sorted data (sortedData.csv)
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */
public class ExportTask implements Task {
    private final static int GLOBAL_CHUNK_SIZE = 64;
    private static ChunkService chunkService;


    public ExportTask(){
    }

    @Override
    public int execute(TaskContext p_ctx) throws IOException {

        // Get Services
        NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
        chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);

        // Get ID
        short ownSlaveID = p_ctx.getCtxData().getSlaveId();
        int ownIndex = Short.toUnsignedInt(ownSlaveID);

        int outputSplit = getIntData(nameService.getChunkID("WO", 1000));

        if( ownIndex == 0){
            int size = getIntData(nameService.getChunkID("SAC0", 1000));
            long[] chunkAddress = getLongArray(nameService.getChunkID("AC0", 1000), size);

            int dataSize = chunkAddress.length;

            if (outputSplit != 1){

                int writeOutIndex;
                for (int i=0; i<outputSplit-1;i++){
                    String filename = "dxapp/data/sortedData"+i+".csv";
                    BufferedWriter outputWriter = new BufferedWriter(new FileWriter(filename));
                    writeOutIndex = i*dataSize/outputSplit;

                    for (int j=0; j<dataSize/outputSplit; j++){
                     outputWriter.write(getIntData(chunkAddress[writeOutIndex+j]) + ", ");
                    }
                    outputWriter.flush();
                    outputWriter.close();
                }

                int name = outputSplit-1;
                String filename = "dxapp/data/sortedData"+name+".csv";
                BufferedWriter outputWriter = new BufferedWriter(new FileWriter(filename));

                writeOutIndex = (outputSplit-1)*dataSize/outputSplit;
                for (int i=writeOutIndex; i<dataSize;i++){
                    outputWriter.write(getIntData(chunkAddress[i]) + ", ");
                }
                outputWriter.flush();
                outputWriter.close();
        } else {
                String filename = "dxapp/data/sortedData.csv";
                BufferedWriter outputWriter = new BufferedWriter(new FileWriter(filename));

                for (long chunkAddress1 : chunkAddress) {
                    outputWriter.write(getIntData(chunkAddress1) + ", ");
                }
                outputWriter.flush();
                outputWriter.close();
            }
        }
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


}
