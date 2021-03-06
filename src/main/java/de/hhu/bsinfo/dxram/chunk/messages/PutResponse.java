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

package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to a PutRequest
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class PutResponse extends Response {
    private byte m_chunkStatusCode;

    /**
     * Creates an instance of PutResponse.
     * This constructor is used when receiving this message.
     */
    public PutResponse() {
        super();
    }

    /**
     * Creates an instance of PutResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *         the request
     * @param p_statusCode
     *         Status code of the chunk
     */
    public PutResponse(final PutRequest p_request, final byte p_statusCode) {
        super(p_request, ChunkMessages.SUBTYPE_PUT_RESPONSE);
        m_chunkStatusCode = p_statusCode;
    }

    @Override
    protected final int getPayloadLength() {
        return Byte.BYTES;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByte(m_chunkStatusCode);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        PutRequest request = (PutRequest) getCorrespondingRequest();

        request.getChunk().setState(
                ChunkState.values()[p_importer.readByte((byte) request.getChunk().getState().ordinal())]);
    }

}
