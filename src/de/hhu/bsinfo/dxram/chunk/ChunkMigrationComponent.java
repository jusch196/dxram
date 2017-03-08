/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.chunk;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Component for chunk handling.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class ChunkMigrationComponent extends AbstractDXRAMComponent {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkMigrationComponent.class.getSimpleName());

    // dependent components
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;
    private LogComponent m_log;

    /**
     * Constructor
     */
    public ChunkMigrationComponent() {
        super(DXRAMComponentOrder.Init.CHUNK, DXRAMComponentOrder.Shutdown.CHUNK);
    }

    /**
     * Puts migrated Chunks
     *
     * @param p_dataStructures
     *     the DataStructures
     * @return whether storing foreign chunks was successful or not
     */
    public boolean putMigratedChunks(final DataStructure[] p_dataStructures) {
        short rangeID = 0;
        int logEntrySize;
        long size = 0;
        long cutChunkID = ChunkID.INVALID_ID;
        short[] backupPeers = null;
        BackupRange backupRange;
        ArrayList<BackupRange> backupRanges;
        ArrayList<Long> cutChunkIDs;

        backupRanges = new ArrayList<>();
        cutChunkIDs = new ArrayList<>();
        m_memoryManager.lockManage();
        for (DataStructure dataStructure : p_dataStructures) {

            m_memoryManager.create(dataStructure.getID(), dataStructure.sizeofObject());
            m_memoryManager.put(dataStructure);

            // #if LOGGER == TRACE
            LOGGER.trace("Stored migrated chunk %s locally", ChunkID.toHexString(dataStructure.getID()));
            // #endif /* LOGGER == TRACE */

            if (m_backup.isActive()) {
                backupRange = m_backup.registerChunk(dataStructure);

                if (rangeID != backupRange.getRangeID()) {
                    backupRanges.add(backupRange);
                    cutChunkIDs.add(dataStructure.getID());
                    rangeID = backupRange.getRangeID();
                }
            }
        }
        m_memoryManager.unlockManage();

        // Send backups after unlocking memory manager lock
        if (m_backup.isActive()) {
            replicateMigratedChunks(p_dataStructures, backupRanges, cutChunkIDs);
        }

        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_log = p_componentAccessor.getComponent(LogComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }

    /**
     * Replicate migrated chunks to corresponding backup ranges
     *
     * @param p_dataStructures
     *     the chunks to replicate
     * @param p_backupRanges
     *     a list of all relevant backup ranges
     * @param p_cutChunkIDs
     *     a list of ChunkIDs. For every listed ChunkID the backup range must be replaced by next element in p_backupRanges
     */
    private void replicateMigratedChunks(final DataStructure[] p_dataStructures, final ArrayList<BackupRange> p_backupRanges,
        final ArrayList<Long> p_cutChunkIDs) {
        int counter = 1;
        short rangeID;
        long cutChunkID;
        short[] backupPeers;
        BackupRange backupRange;

        backupRange = p_backupRanges.get(0);
        cutChunkID = p_cutChunkIDs.get(0);
        backupPeers = backupRange.getBackupPeers();
        rangeID = backupRange.getRangeID();

        for (DataStructure dataStructure : p_dataStructures) {

            if (dataStructure.getID() == cutChunkID) {
                backupRange = p_backupRanges.get(counter);
                cutChunkID = p_cutChunkIDs.get(counter);
                counter++;

                backupPeers = backupRange.getBackupPeers();
                rangeID = backupRange.getRangeID();
            }

            for (short backupPeer : backupPeers) {
                if (backupPeer != NodeID.INVALID_ID) {
                    try {
                        m_network.sendMessage(new LogMessage(backupPeer, rangeID, dataStructure));
                    } catch (final NetworkException ignore) {

                    }
                }
            }
        }
    }

}
