/* 
 * Copyright (C) 2013 Lisa Park, Inc. (www.lisa-park.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.lisapark.octopus.core.compiler.akka;

import org.lisapark.octopus.core.compiler.esper.*;
import com.espertech.esper.client.EPRuntime;
import com.google.common.collect.Maps;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.runtime.SinkContext;
import org.lisapark.octopus.core.sink.external.CompiledExternalSink;
import org.lisapark.octopus.util.Pair;
import org.lisapark.octopus.util.esper.EsperUtils;

import java.util.Arrays;
import java.util.Map;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
class AkkaExternalSinkAdaptor {
    private final CompiledExternalSink externalSink;
    private final Pair<String, Integer>[] sourceIdToInputId;

    private final SinkContext ctx;
    private final EPRuntime runtime;

    @SuppressWarnings("unchecked")
    AkkaExternalSinkAdaptor(CompiledExternalSink externalSink, SinkContext ctx, EPRuntime runtime) {
        this.externalSink = externalSink;
        this.ctx = ctx;
        this.runtime = runtime;

        this.sourceIdToInputId = (Pair<String, Integer>[]) new Pair[externalSink.getInputs().size()];

        int index = 0;
        for (Input input : externalSink.getInputs()) {
            String sourceId = EsperUtils.getEventNameForSource(input.getSource());
            Integer inputId = input.getId();
            sourceIdToInputId[index++] = Pair.newInstance(sourceId, inputId);
        }
    }

    Pair<String, Integer>[] getSourceIdToInputId() {
        return Arrays.copyOf(sourceIdToInputId, sourceIdToInputId.length);
    }

    public void update(Map<String, Object> eventFromInput_1) {
        Event event = new Event(eventFromInput_1);
        Map<Integer, Event> eventsByInputId = Maps.newHashMapWithExpectedSize(1);
        eventsByInputId.put(sourceIdToInputId[0].getSecond(), event);

        externalSink.processEvent(ctx, eventsByInputId);
    }

    public void update(Event eventFromInput_1, Event eventFromInput_2) {
        // todo multiple inputs for sinks?
    }
}
