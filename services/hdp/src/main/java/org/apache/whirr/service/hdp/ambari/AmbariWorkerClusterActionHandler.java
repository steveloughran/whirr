/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.whirr.service.hdp.ambari;

import org.apache.whirr.Cluster;
import org.apache.whirr.service.ClusterActionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public final class AmbariWorkerClusterActionHandler extends AbstractAmbariClusterActionHandler {


  private static final Logger LOG =
    LoggerFactory.getLogger(AmbariWorkerClusterActionHandler.class);


  @Override
  public String getRole() {
    return AMBARI_WORKER;
  }


  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
    Cluster.Instance serverInstance = extractAmbariServer(event);

  }

  @Override
  protected void afterStart(ClusterActionEvent event) throws IOException, InterruptedException {
    Set<Cluster.Instance> instances = getAmbariWorkers(event.getCluster());
    LOG.info("Started {} ambari workers", instances.size());
  }
}
