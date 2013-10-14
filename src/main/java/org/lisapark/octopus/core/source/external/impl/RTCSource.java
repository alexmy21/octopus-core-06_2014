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
package org.lisapark.octopus.core.source.external.impl;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.parameter.Parameter.Builder;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;

/**
 *
 * @author alex (alexmy@lisa-park.com)
 */
@Persistable
public class RTCSource extends ExternalSource {

    private static final String DEFAULT_NAME = "RTC Source";
    private static final String DEFAULT_DESCRIPTION = "Run Time Container Source Processor. Initiates Chained Model Work Flow.";
    private static final int RTC_SIGNAL_NAME_PARAMETER_ID = 1;
    private static final int RTC_SIGNAL_VALUE_PARAMETER_ID = 2;

    public RTCSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private RTCSource(UUID id, RTCSource copyFromSource) {
        super(id, copyFromSource);
    }

    public RTCSource(RTCSource copyFromSource) {
        super(copyFromSource);
    }

    public String getStartSignalName() {
        return getParameter(RTC_SIGNAL_NAME_PARAMETER_ID).getValueAsString();
    }

    public Boolean getStartSignalValue() {
        return (Boolean) getParameter(RTC_SIGNAL_VALUE_PARAMETER_ID).getValue();
    }

    @Override
    public RTCSource copyOf() {
        return new RTCSource(this);
    }

    @Override
    public RTCSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new RTCSource(sourceId, this);
    }

    public static RTCSource newTemplate() {
        UUID sourceId = UUID.randomUUID();

        RTCSource rtcSource = new RTCSource(sourceId, "RTC Source", "Generates run token to start all Octopus model's"
                + " containers that are connected to the source.");
        rtcSource.setOutput(Output.outputWithId(1).setName("Output data"));

        rtcSource.addParameter(Parameter.stringParameterWithIdAndName(RTC_SIGNAL_NAME_PARAMETER_ID, "Run signal name:")
                .description("Run signal/token name.")
                .defaultValue("attr")
                .required(true));

        rtcSource.addParameter(Parameter.booleanParameterWithIdAndName(RTC_SIGNAL_VALUE_PARAMETER_ID, "Run signal value:")
                .description("Run signal value.")
//                .defaultValue(true)
                .required(true));

        return rtcSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledRedisSource(copyOf());
    }

    class CompiledRedisSource implements CompiledExternalSource {

        protected final Logger logger = Logger.getLogger(CompiledRedisSource.class.getName());
        protected final RTCSource source;
        protected volatile boolean running;
        protected Thread thread;

        public CompiledRedisSource(RTCSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) {
            EventType eventType = this.source.getOutput().getEventType();
            List attributes = eventType.getAttributes();
            int numberEventsCreated = 0;

            Map attributeData = Maps.newHashMap();
            attributeData.put(this.source.getStartSignalName(), this.source.getStartSignalValue());

            runtime.sendEventFromSource(new Event(attributeData), this.source);
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }
    }
}
