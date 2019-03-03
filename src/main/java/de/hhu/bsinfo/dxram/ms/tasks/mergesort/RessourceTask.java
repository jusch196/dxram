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

/**
 * Task to get available ressources
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de
 */
public class RessourceTask implements Task{


        public RessourceTask() {
        }

        @Override
        public int execute(final TaskContext p_ctx) {

                // Get services
                ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
                NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

                // Get ID
                short ownSlaveID = p_ctx.getCtxData().getSlaveId();
                long[] ressourceChunk = new long[1];

                chunkService.create().create(p_ctx.getCtxData().getOwnNodeId(), ressourceChunk, 1, 64);
                ByteBuffer byteBuffer = ByteBuffer.allocate(64);
                byteBuffer.putInt(Runtime.getRuntime().availableProcessors());
                ChunkByteArray chunkByteArray = new ChunkByteArray(ressourceChunk[0], byteBuffer.array());
                chunkService.put().put(chunkByteArray);

                nameService.register(ressourceChunk[0], "RC-" + Short.toUnsignedInt(ownSlaveID));
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


