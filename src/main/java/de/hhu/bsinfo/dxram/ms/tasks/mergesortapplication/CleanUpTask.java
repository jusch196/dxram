package de.hhu.bsinfo.dxram.ms.tasks.mergesortapplication;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.io.IOException;

public class CleanUpTask implements Task {
    @Override
    public int execute(TaskContext p_ctx) throws IOException {
        System.out.println("Starte Cleanup");

        if (p_ctx.getCtxData().getSlaveId() == 0){
            ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
            NameserviceService nameserviceService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
            int numberOfWorkerNodes = p_ctx.getCtxData().getSlaveNodeIds().length;

            chunkService.remove().remove(nameserviceService.getChunkID("GT", 100));
            chunkService.remove().remove(nameserviceService.getChunkID("WO", 100));

            for (int i=0; i<numberOfWorkerNodes; i++){
                chunkService.remove().remove(nameserviceService.getChunkID("SID"+i, 100));
                chunkService.remove().remove(nameserviceService.getChunkID("SAC"+i, 100));
                chunkService.remove().remove(nameserviceService.getChunkID("AC"+i, 100));
                chunkService.remove().remove(nameserviceService.getChunkID("RC"+i, 100));
            }
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
}
