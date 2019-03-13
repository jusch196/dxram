package de.hhu.bsinfo.dxram.ms.tasks.mergesortapplication;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.nio.ByteBuffer;

/**
 * Task to get available resources and connect their SlaveID with their NodeID
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de
 */
public class ResourceTask implements Task{
        private static final int GLOBAL_CHUNK_SIZE = 64;


        public ResourceTask() {
        }

        @Override
        public int execute(final TaskContext p_ctx) {

                // Get services
                ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
                NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

                // Get ID
                short ownSlaveID = p_ctx.getCtxData().getSlaveId();
                short ownNodeID = p_ctx.getCtxData().getOwnNodeId();

                long[] resourceChunk = new long[1];

                // register resources
                chunkService.create().create(p_ctx.getCtxData().getOwnNodeId(), resourceChunk, 1, GLOBAL_CHUNK_SIZE);

                ByteBuffer byteBuffer = ByteBuffer.allocate(GLOBAL_CHUNK_SIZE);
                byteBuffer.putInt(Runtime.getRuntime().availableProcessors());

                ChunkByteArray chunkByteArray = new ChunkByteArray(resourceChunk[0], byteBuffer.array());
                chunkService.put().put(chunkByteArray);
                nameService.register(resourceChunk[0], "RC" + ownSlaveID);

                // connect SlaveID with NodeID
                chunkService.create().create(p_ctx.getCtxData().getOwnNodeId(), resourceChunk, 1, GLOBAL_CHUNK_SIZE);

                byteBuffer = ByteBuffer.allocate(GLOBAL_CHUNK_SIZE);
                byteBuffer.putShort(ownNodeID);

                chunkByteArray = new ChunkByteArray(resourceChunk[0], byteBuffer.array());
                chunkService.put().put(chunkByteArray);
                nameService.register(resourceChunk[0], "SID" + ownSlaveID);

                return 0;
        }

        @Override
        public void handleSignal(final Signal p_signal) {
            // Doesn't handle signals
        }

        @Override
        public void exportObject(final Exporter p_exporter) {

        }

        @Override
        public void importObject(final Importer p_importer) {

        }

        @Override
        public int sizeofObject() {
            return 0;
        }
 }


