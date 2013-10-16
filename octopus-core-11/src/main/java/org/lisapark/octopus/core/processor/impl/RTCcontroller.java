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
package org.lisapark.octopus.core.processor.impl;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.UUID;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.lisapark.octopus.ProgrammerException;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.processor.CompiledProcessor;
import org.lisapark.octopus.core.processor.Processor;
import org.lisapark.octopus.core.processor.ProcessorInput;
import org.lisapark.octopus.core.processor.ProcessorOutput;
import org.lisapark.octopus.core.runtime.ProcessorContext;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class RTCcontroller extends Processor<Void> {

    private static final String DEFAULT_NAME = "RTC Controller";
    private static final String DEFAULT_DESCRIPTION = "Run Time Container Controller - runs provided list of models on incoming signal"
            + " and sends signal to the connected processor when all models are done.";

    private static final int OCTOPUS_SERVER_URL_PARAMETER_ID = 1;
    private static final int MODEL_NAME_LIST_PARAMETER_ID = 2;
    private static final int MODEL_NAME_FIELD_PARAMETER_ID = 3;

    private static final String DEFAULT_INPUT_DESCRIPTION = "Field name";
    private static final String DEFAULT_OUTPUT_DESCRIPTION = "Counter name.";

    /**
     * Pipe takes a single input
     */
    private static final int INPUT_ID = 1;
    private static final int OUTPUT_ID = 1;

    protected RTCcontroller(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected RTCcontroller(UUID id, RTCcontroller copyFromPipe) {
        super(id, copyFromPipe);
    }

    protected RTCcontroller(RTCcontroller copyFromPipe) {
        super(copyFromPipe);
    }

    public ProcessorInput getInput() {
        // there is only one input for a Pipe
        return getInputs().get(0);
    }

    @Override
    public RTCcontroller newInstance() {
        return new RTCcontroller(UUID.randomUUID(), this);
    }

    @Override
    public RTCcontroller copyOf() {
        return new RTCcontroller(this);
    }

    public String getServerUrl() {
        return getParameter(OCTOPUS_SERVER_URL_PARAMETER_ID).getValueAsString();
    }

    public String getModelNameList() {
        return getParameter(MODEL_NAME_LIST_PARAMETER_ID).getValueAsString();
    }

    public String getModelNameField() {
        return getParameter(MODEL_NAME_FIELD_PARAMETER_ID).getValueAsString();
    }

    /**
     * Validates and compile this Pipe. Doing so takes a "snapshot" of the
     * {@link #getInputs()} and {@link #output} and returns a
     * {@link CompiledProcessor}.
     *
     * @return CompiledProcessor
     * @throws org.lisapark.octopus.core.ValidationException
     */
    @Override
    public CompiledProcessor<Void> compile() throws ValidationException {
        validate();

        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        RTCcontroller copy = copyOf();
        return new CompiledRTCcontroller(copy);
    }

    /**
     * Returns a new {@link Pipe} processor configured with all the appropriate
     * {@link org.lisapark.octopus.core.parameter.Parameter}s, {@link Input}s
     * and {@link Output}.
     *
     * @return new {@link Pipe}
     */
    public static RTCcontroller newTemplate() {
        UUID processorId = UUID.randomUUID();
        RTCcontroller rtc = new RTCcontroller(processorId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        // only a single boolean input
        rtc.addInput(
                ProcessorInput.booleanInputWithId(INPUT_ID).name("Start").description(DEFAULT_INPUT_DESCRIPTION)
        );

        rtc.addParameter(Parameter.stringParameterWithIdAndName(OCTOPUS_SERVER_URL_PARAMETER_ID, "Octopus Server URL")
                .description("Octopus server URL including port number.")
                .defaultValue("http://10.1.10.10:8084/run/run")
                .required(true));

        rtc.addParameter(Parameter.stringParameterWithIdAndName(MODEL_NAME_LIST_PARAMETER_ID, "Model Name List")
                .description("Comma separated Model names as they are in db4o database.")
                .defaultValue("")
                .required(true));

        rtc.addParameter(Parameter.stringParameterWithIdAndName(MODEL_NAME_FIELD_PARAMETER_ID, "Model Name Field")
                .description("Model name field that is used to output log data.")
                .defaultValue("modelname")
                .required(true));

        // double output
        try {
            rtc.setOutput(
                    ProcessorOutput.booleanOutputWithId(OUTPUT_ID).name("Done").description(DEFAULT_OUTPUT_DESCRIPTION).attributeName("done")
            );
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the Pipe with an invalid attriubte name
            throw new ProgrammerException(ex);
        }

        return rtc;
    }

    /**
     * This {@link CompiledProcessor} is the actual logic that implements the
     * Simple Moving Average.
     */
    static class CompiledRTCcontroller extends CompiledProcessor<Void> {

        RTCcontroller rtc;

        protected CompiledRTCcontroller(RTCcontroller rtc) {
            super(rtc);

            this.rtc = rtc;
        }

        @Override
        public Object processEvent(ProcessorContext<Void> ctx, Map<Integer, Event> eventsByInputId) {

            Event startEvent = eventsByInputId.get(INPUT_ID);
            String startName = rtc.getInputById(INPUT_ID).getSourceAttributeName();
            Boolean start = startEvent.getAttributeAsBoolean(startName);

            if (!start) {
                return null;
            }

            Boolean done = null;

            // Create an HttpClient with the ThreadSafeClientConnManager.
            // This connection manager must be used if more than one thread will
            // be using the HttpClient.
            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
            cm.setMaxTotal(100);

            CloseableHttpClient httpclient = HttpClients.custom().setConnectionManager(cm).build();

            String[] modelList = rtc.getModelNameList().split(",");
            String modelField = rtc.getModelNameField();

            try {
                // create a thread for each URI
                GetThread[] threads = new GetThread[modelList.length];
                Map attributeData = Maps.newHashMap();

                for (int i = 0; i < threads.length; i++) {

                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put(modelField, modelList[i].trim());
                    jsonObject.put("modeljson", "");

                    HttpPost httpPost = new HttpPost(rtc.getServerUrl());

                    httpPost.setHeader("id", rtc.getName());
                    httpPost.setHeader("name", rtc.getName());

                    httpPost.setHeader("Content-Type", "application/json");

                    try {
                        StringEntity entity = new StringEntity(jsonObject.toString(), HTTP.UTF_8);
                        httpPost.setEntity(entity);

                        threads[i] = new GetThread(httpclient, httpPost, modelField, attributeData, i + 1);
                    } catch (UnsupportedEncodingException ex) {
                        Exceptions.printStackTrace(ex);
                    }
                }

                for (GetThread thread : threads) {
                    thread.start();
                }
                for (GetThread thread : threads) {
                    thread.join();
                }

                startEvent.getData().put(modelField, modelList);
                
                done = true;

            } catch (IllegalStateException ex) {
                Exceptions.printStackTrace(ex);
            } catch (JSONException ex) {
                Exceptions.printStackTrace(ex);
            } catch (InterruptedException ex) {
                Exceptions.printStackTrace(ex);
            } finally {
                if (httpclient != null) {
                    try {
                        httpclient.close();
                    } catch (IOException ex) {
                        Exceptions.printStackTrace(ex);
                    }
                }
            }
            return done;
        }
    }

    static class GetThread extends Thread {

        private CloseableHttpClient httpClient;
        private HttpPost httpPost;
        private HttpContext context;
        private String modelField;
        private Map attributeData;
        private final int id;

        public GetThread(CloseableHttpClient httpClient, HttpPost httpPost, String modelField, Map attributeData, int id) {
            this.httpClient = httpClient;
            this.httpPost = httpPost;
            this.context = new BasicHttpContext();
            this.modelField = modelField;
            this.attributeData = attributeData;
            this.id = id;
        }

        /**
         * Executes the GetMethod and prints some satus information.
         */
        @Override
        public void run() {
            try {
                
                CloseableHttpResponse  httpResponse = httpClient.execute(httpPost);

                attributeData = Maps.newHashMap();

                attributeData.put(modelField, httpPost.getURI());
                attributeData.put("httpResponse", httpResponse);

            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            }
        }
    }
}
