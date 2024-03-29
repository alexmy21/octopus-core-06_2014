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
package org.lisapark.octopus.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import java.util.List;
import java.util.Set;

/**
 * @author Alex
 */
public class ModelBean {

    public static final String MODEL_NAME = "modelName";
    public static final String AUTHOR_EMAIL = "authorEmail";
    public static final String DESCRIPTION = "description";
    public static final String MODEL_JSON = "modelJson";
    private String modelName;
    private String authorEmail;
    private String description;
    private Set<String> sources;
    private Set<String> processors;
    private Set<String> sinks;

    /**
     *
     * @return
     */
    public String getModelName() {
        return modelName;
    }

    /**
     *
     * @return
     */
    public String getAuthorEmail() {
        return authorEmail == null ? "" : authorEmail;
    }

    public void setAuthorEmail(String authorEmail) {
        this.authorEmail = authorEmail;
    }

    public String getDescription() {
        return description == null ? "" : description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setModelName(String name) {
        this.modelName = name;
    }

    public Set<String> getSources() {
        return sources;
    }

    public void setSources(Set<String> sources) {
        this.sources = sources;
    }

    public Set<String> getProcessors() {
        return processors;
    }

    public void setProcessors(Set<String> processors) {
        this.processors = processors;
    }

    public Set<String> getSinks() {
        return sinks;
    }

    public void setSinks(Set<String> sinks) {
        this.sinks = sinks;
    }

    /**
     *
     * @param bean
     * @return
     */
    public Double compare(ModelBean bean) {
        
        Double tolerance = null;
        List<Double> comp = Lists.newArrayList();
        
        comp.add(compareBySources(bean));
        comp.add(compareBySinks(bean));
        comp.add(compareByProcessors(bean));
        
        double sum = 0;
        int count = 0;
        
        for (Double item : comp) {
            if (item != null) {
                sum += item;
                count++;
            }
        }
        
        if (count > 0) {
            tolerance = sum / count;
        }
        
        return tolerance;
    }

    /**
     *
     * @param bean
     * @return
     */
    public Double compareBySources(ModelBean bean) {
        
        ProcessorBean thisSource = new Gson().fromJson(this.getSources().iterator().next(), ProcessorBean.class);
        ProcessorBean thatSource = new Gson().fromJson(bean.getSources().iterator().next(), ProcessorBean.class);
        
        return thisSource.compare(thatSource);
    }

    /**
     *
     * @param bean
     * @return
     */
    public Double compareBySinks(ModelBean bean) {
        
        ProcessorBean thisSink = new Gson().fromJson(this.getSinks().iterator().next(), ProcessorBean.class);
        ProcessorBean thatSink = new Gson().fromJson(bean.getSinks().iterator().next(), ProcessorBean.class);
        
        return thisSink.compare(thatSink);
    }

    /**
     *
     * @param bean
     * @return
     */
    public Double compareByProcessors(ModelBean bean) {
        
        Double tolerance = null;
        
        Set<String> thisProcs = this.getProcessors();
        Set<String> thatProcs = bean.getProcessors();
        
        int diff = Sets.symmetricDifference(thisProcs, thatProcs).size();
        int union = Sets.union(thisProcs, thatProcs).size();
        
        if (union > 0) {
            tolerance = (double) diff / (double) union;
        }
        
        return tolerance;
    }
}
