package de.hhu.bsinfo.dxcompute.bench;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.chunk.ChunkIDRangeUtils;

import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;

/**
 * Created by burak on 19.01.17.
 */
public class NameserviceRegisterTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(NameserviceRegisterTask.class.getSimpleName());

    @Expose
    private int m_numThreads = 1;
    @Expose
    private int m_chunkCount = 1000;



    public NameserviceRegisterTask() {

    }

    @Override
    public int execute(TaskContext p_ctx) {


        NameserviceService nameserviceService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

        long[] chunkCountsPerThread = ChunkIDRangeUtils.distributeChunkCountsToThreads(m_chunkCount, m_numThreads);

        Thread[] threads = new Thread[m_numThreads];
        long[] timeStart = new long[m_numThreads];
        long[] timeEnd = new long[m_numThreads];


        System.out.printf("Registering %d chunks with %d thread(s)...\n",
                m_chunkCount, m_numThreads);

        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            threads[i] = new Thread(() -> {

                String[] randomNames = new String[(int)chunkCountsPerThread[threadIdx]];
                for (int k = 0; k < chunkCountsPerThread[threadIdx]; k++) {
                    randomNames[k] = getRandomValue();
                }

                timeStart[threadIdx] = System.nanoTime();

                for (int j = 0; j < chunkCountsPerThread[threadIdx]; j++) {
                    nameserviceService.register(1, randomNames[j]);
                }

                timeEnd[threadIdx] = System.nanoTime();
            });

        }

        for (Thread thread : threads) {
            thread.start();
        }

        boolean threadJoinFailed = false;
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (final InterruptedException e) {
                LOGGER.error("Joining thread failed", e);
                threadJoinFailed = true;
            }
        }

        if (threadJoinFailed) {
            return -1;
        }

        System.out.print("Times per thread:");
        for (int i = 0; i < m_numThreads; i++) {
            System.out.printf("\nThread-%d: %f sec", i, (timeEnd[i] - timeStart[i]) / 1000.0 / 1000.0 / 1000.0);
        }
        System.out.println();

        // total time is measured by the slowest thread
        long totalTime = 0;
        for (int i = 0; i < m_numThreads; i++) {
            long time = timeEnd[i] - timeStart[i];
            if (time > totalTime) {
                totalTime = time;
            }
        }

        System.out.printf("Total time: %f sec\n", totalTime / 1000.0 / 1000.0 / 1000.0);
        System.out.printf("Throughput: %f chunks/sec\n", 1000.0 * 1000.0 * 1000.0 / ((double) totalTime / m_chunkCount));

        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_numThreads);
        p_exporter.writeInt(m_chunkCount);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_numThreads = p_importer.readInt();
        m_chunkCount = p_importer.readInt();
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 2;
    }


    // vieleicht in NameServiceUtils auslagen
    private String allowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGRHIJKLMNOPQRSTUVWXYZ0123456789-";

    private String getRandomValue() {
        Random random = new Random();
        int length = random.nextInt(5 - 0 + 1) + 0;
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < length; i++) {
            sb.append(allowedChars.charAt(random.nextInt(allowedChars.length())));
        }
        return sb.toString();
    }

}