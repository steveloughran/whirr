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

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.service.ClusterActionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class AmbariWorkerClusterActionHandler extends AbstractAmbariClusterActionHandler {


  private static final Logger LOG =
    LoggerFactory.getLogger(AmbariWorkerClusterActionHandler.class);


  /**
   * return {@link #AMBARI_WORKER} always
   * @return the role.
   */
  @Override
  public String getRole() {
    return AMBARI_WORKER;
  }


  /**
   * Pre-boostrap actions. The keys are created and validated here, even though it could be left to
   * the server instances, because the order in which servers are deployed varied -the workers
   * can do their work first.
   * @param event event to process
   * @throws IOException IO problems
   * @throws InterruptedException interrupted operations.
   */
  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException, InterruptedException {
    Configuration conf = getConfiguration(event);
    createOrValidateKeys(conf);
  }


  /**
   * Pre-config: any final validation and actions for cluster config
   * @param event event to process
   * @throws IOException IO problems
   * @throws InterruptedException interrupted operations.
   */
  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {

    //here the cluster should be set up
    if (OPTION_CLUSTER_MUST_HAVE_SERVER) {
      extractAmbariServer(event);
    }

    Configuration conf = event.getClusterSpec().getConfiguration();
    addStatement(event, createKeyAuthStatement(conf));

  }

}
