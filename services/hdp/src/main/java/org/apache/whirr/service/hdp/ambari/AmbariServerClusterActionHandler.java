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
import org.apache.commons.io.FileUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager;
import org.apache.whirr.service.hdp.BadDeploymentException;
import org.apache.whirr.service.hdp.hadoop.HdpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.jclouds.scriptbuilder.domain.Statements.call;

public final class AmbariServerClusterActionHandler extends AbstractAmbariClusterActionHandler {

  private static final Logger LOG =
    LoggerFactory.getLogger(AmbariServerClusterActionHandler.class);


  /**
   * return {@link #AMBARI_SERVER} always
   * @return the role.
   */
  @Override
  public String getRole() {
    return AMBARI_SERVER;
  }


  /**
   * Boostrap actions: set up what is needed to install the server
   * @param event event to process
   * @throws IOException IO problems
   */
  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();

    Configuration conf = getConfiguration(clusterSpec);
    String installFunction = conf.getString(
      KEY_INSTALL_FUNCTION,
      FUNCTION_INSTALL);
    addStatement(event, call(RETRY_HELPERS));
    addStatement(event, call(HdpConstants.HDP_REGISTER_REPO_FUNCTION));
    addStatement(event, call(AMBARI_FUNCTIONS));

    addStatement(event, call(installFunction, AMBARI_SERVER));

    createOrValidateKeys(conf);
  }

  /**
   * Pre-config: any final validation and actions for cluster config
   * @param event event to process
   * @throws IOException IO problems
   * @throws InterruptedException interrupted operations.
   */

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException,
                                                                  InterruptedException {

    Cluster.Instance serverInstance = extractAmbariServer(event);

    LOG.info("Authorizing firewall");
    event.getFirewallManager().addRules(
      FirewallManager.Rule.create().destination(serverInstance).ports(AMBARI_SERVER_WEB_UI_PORT));

    ClusterSpec clusterSpec = event.getClusterSpec();

    Configuration conf = getConfiguration(clusterSpec);
    String domain = conf.getString(KEY_INTERNAL_DOMAIN_NAME, "");
    if(domain.isEmpty()) {
      String msg = "No value provided for " + KEY_INTERNAL_DOMAIN_NAME + " -the worker file and HTTPS scripts would be invalid";
      LOG.warn(msg);
      throw new BadDeploymentException(msg);
    }
    String configureFunction = conf.getString(
      KEY_CONFIGURE_FUNCTION,
      FUNCTION_POST_CONFIGURE);

    addStatement(event, call(RETRY_HELPERS));
    addStatement(event, call(AMBARI_FUNCTIONS));

    addStatement(event, call(configureFunction, AMBARI_SERVER));
    addStatement(event, createKeyAuthStatement(conf));

  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());
    createProxyScript(clusterSpec, cluster);
  }


  /**
   * Pre-start -add the actions to start the service
   * @param event event to process
   * @throws IOException IO problems
   * @throws InterruptedException interrupted operations.
   */

  @Override
  protected void beforeStart(ClusterActionEvent event) throws IOException, InterruptedException {
    super.beforeStart(event);
    addStatement(event, call(RETRY_HELPERS));
    addStatement(event, call(AMBARI_FUNCTIONS));
    addStatement(event, call(AMBARI_START, AMBARI_SERVER));
  }

  /**
   * After start -output the file listing all the workers; print that and the keyfile location.
   * @param event event to process
   * @throws IOException IO problems
   * @throws InterruptedException interrupted operations.
   */

  @Override
  protected void afterStart(ClusterActionEvent event) throws IOException, InterruptedException {
    super.afterStart(event);
    Cluster cluster = event.getCluster();
    URL ambariURL = getAmbariServerURL(cluster);
    LOG.info("Ambari web UI available at {}", ambariURL);
    Configuration conf = getConfiguration(event.getClusterSpec());
    String workerDestFilename = conf.getString(KEY_WORKER_DEST_FILE, null);
    //get the domain and add a . if needed
    String domain = conf.getString(KEY_INTERNAL_DOMAIN_NAME, "");
    if (!domain.isEmpty() && !domain.startsWith(".")) {
      domain = "." + domain;
    }
    String workerList = Utils.createWorkerDescriptionFile(cluster, domain);
    LOG.info("Worker list:\n{}", workerList);
    if (workerDestFilename != null) {
      File destFile = new File(workerDestFilename);
      try {
        FileUtils.writeStringToFile(destFile, workerList);
        LOG.info("Worker list file: {}", destFile);
      } catch (IOException e) {
        LOG.error("Failed to write worker list to " + destFile, e);
      }
    }
    File keyFile = bindToPrivateKeyFile(conf);
    LOG.info("Private Key file for worker access is : " + keyFile.getAbsolutePath());
  }

  /**
   * Stop the server
   * @param event event to process
   * @throws IOException IO problems
   * @throws InterruptedException interrupted operations.
   */

  @Override
  protected void beforeStop(ClusterActionEvent event) throws IOException, InterruptedException {
    super.beforeStop(event);
    addStatement(event, call(RETRY_HELPERS));
    addStatement(event, call(AMBARI_FUNCTIONS));
    addStatement(event, call(AMBARI_STOP, AMBARI_SERVER));
  }


}
