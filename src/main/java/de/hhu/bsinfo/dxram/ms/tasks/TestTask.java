package de.hhu.bsinfo.dxram.ms.tasks;

import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class TestTask implements Task {

    /**
     * Constructor
     */
    public TestTask() {

    }

    @Override
    public int execute(final TaskContext p_ctx) {
        System.out.println("Task ausgeführt!");
        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
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
