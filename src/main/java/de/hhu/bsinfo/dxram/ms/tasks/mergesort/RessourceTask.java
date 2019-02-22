package de.hhu.bsinfo.dxram.ms.tasks.mergesort;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameServiceIndexData;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

public class RessourceTask implements Task{
        /**
         * Constructor
         */
        @Expose
        private long[][] partition;

        @Expose
        private List <Short> onlineWorkerNodeIDs;

        public RessourceTask() {
        }



        @Override
        public int execute(final TaskContext p_ctx) {

                System.out.println("Starte Ressource-Task");

                ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
                NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

                short[] slaveNodeIds = p_ctx.getCtxData().getSlaveNodeIds();
                short ownNodeID = p_ctx.getCtxData().getOwnNodeId();
                short ownSlaveID = p_ctx.getCtxData().getSlaveId();

                System.out.println("Eigene SlaveNodeID: " + NodeID.toHexString(ownSlaveID));
                System.out.println("Eigene NodeID: " + NodeID.toHexString(ownNodeID));
                System.out.println("Andere SlaveNodeIDS: " + NodeID.nodeIDArrayToString(slaveNodeIds));
                System.out.println("Eigener Index " + Short.toUnsignedInt(ownSlaveID));

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

        private int getIntData(long chunkId, int size, ChunkService chunkService){
                ChunkByteArray chunk = new ChunkByteArray(chunkId, size);
                chunkService.get().get(chunk);
                byte[] byteData = chunk.getData();
                return ByteBuffer.wrap(byteData).getInt();
        }
    }


