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
package org.lisapark.octopus.repository.neo4j;

import org.lisapark.octopus.core.ModelBean;

import org.lisapark.octopus.core.ProcessorBean;

import org.neo4j.rest.graphdb.RestAPI;

import org.neo4j.rest.graphdb.RestGraphDatabase;

/**
 * 
 * @author alexmy
 */
public class OctopusNeo4jRepository extends RestGraphDatabase {

    public OctopusNeo4jRepository(RestAPI restApi) {

        super(restApi);

    }

    public OctopusNeo4jRepository(String uri) {

        super(uri);

    }

    public OctopusNeo4jRepository(String uri, String userId, String password) {

        super(uri, userId, password);

    }

    /**
     * 
     * @param bean
     * @return 
     */
    public Long addProcessor(ProcessorBean bean) {

        Long nodeId = processorExists(bean.getClassName());

        if (nodeId == null) {

            nodeId = createProcessorNode(bean);

        }

        return nodeId;

    }

    /**
     * 
     * @param className
     * @return 
     */
    public ProcessorBean getProcessorByClassName(String className) {

        ProcessorBean bean = null;

        return bean;

    }

    /**
     * 
     * @param bean
     * @return 
     */
    public Long addModel(ModelBean bean) {

        Long nodeId = modelExists(bean.getModelName());

        if (nodeId == null) {

            nodeId = createModel(bean);

            if (addModel2Cluster(bean) == null) {

                Long clusterId = createCluster(bean);

            }

        }

        return nodeId;

    }

    /**
     * 
     * @param name
     * @return 
     */
    public ModelBean getModelBeanByName(String name) {

        ModelBean bean = null;

        return bean;

    }

    /**
     * 
     * @param bean
     * @return 
     */
    private Long createCluster(ModelBean bean) {

        Long nodeId = null;

        return nodeId;

    }

    public Long addModel2Cluster(ModelBean bean) {

        Long nodeId = null;

        return nodeId;

    }

    public Long addCluster2Cluster(ModelBean bean) {

        Long nodeId = null;

        return nodeId;

    }

    public void updateClasterContext(Long clusterId, String contextType) {

    }

    public void updateAllClasterContexts(Long clusterId) {

    }

    public Long processorExists(String className) {

        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.

    }

    public Long createProcessorNode(ProcessorBean bean) {

        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.

    }

    private Long modelExists(String modelName) {

        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.

    }

    private Long createModel(ModelBean bean) {

        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.

    }

}
