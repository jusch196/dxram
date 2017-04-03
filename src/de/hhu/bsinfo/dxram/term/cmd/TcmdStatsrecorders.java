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

import java.util.Collection;

import de.hhu.bsinfo.dxram.stats.Statistics;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;

/**
 * Prints all available statistics recorders
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdStatsrecorders extends TerminalCommand {
    public TcmdStatsrecorders() {
        super("statsrecorders");
    }

    @Override
    public String getHelp() {
        return "Prints all available statistics recorders\n" + "Usage: statsrecorders()";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        Collection<StatisticsRecorder> recorders = Statistics.getRecorders();

        for (StatisticsRecorder recorder : recorders) {
            p_ctx.printfln("> %s", recorder.getName());
        }
    }
}
