/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.term.cmd;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;
import java.util.List;

/**
 * Prints a summary of the specified superpeer's metadata
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdMetadata extends TerminalCommand {
    public TcmdMetadata() {
        super("metadata");
    }

    @Override
    public String getHelp() {
        return "Prints a summary of the specified superpeer's metadata\n" +
                "Usage: metadatasummary <nid>\n" +
                "  nid: Node id of the superpeer to print the metadata of\n" +
                "       \"all\" prints metadata of all superpeers";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        String nidStr = p_ctx.getArgString(p_args, 0, null);

        if (nidStr == null) {
            p_ctx.printlnErr("No nid specified");
            return;
        }

        if ("all".equals(nidStr)) {
            LookupService lookup = p_ctx.getService(LookupService.class);
            BootService boot = p_ctx.getService(BootService.class);
            List<Short> nodeIds = boot.getOnlineNodeIDs();

            for (Short nodeId : nodeIds) {
                NodeRole curRole = boot.getNodeRole(nodeId);
                if (curRole == NodeRole.SUPERPEER) {
                    String summary = lookup.getMetadataSummary(nodeId);
                    p_ctx.printfln("Metadata summary of 0x%X:\n%s", nodeId, summary);
                }
            }
        } else {
            short nid = p_ctx.getArgNodeId(p_args, 0, NodeID.INVALID_ID);

            if (nid == NodeID.INVALID_ID) {
                p_ctx.printlnErr("No nid specified");
                return;
            }

            LookupService lookup = p_ctx.getService(LookupService.class);

            String summary = lookup.getMetadataSummary(nid);
            p_ctx.printfln("Metadata summary of 0x%X:\n%s", nid, summary);
        }
    }
}
