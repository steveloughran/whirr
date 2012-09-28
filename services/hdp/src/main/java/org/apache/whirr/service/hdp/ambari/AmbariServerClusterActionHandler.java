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
import org.apache.whirr.service.hdp.hadoop.HdpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.Set;

import static org.apache.whirr.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

public final class AmbariServerClusterActionHandler extends AbstractAmbariClusterActionHandler {

  private static final Logger LOG =
    LoggerFactory.getLogger(AmbariServerClusterActionHandler.class);


  @Override
  public String getRole() {
    return AMBARI_SERVER;
  }


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
    addStatement(event, createKeyAuthStatement(conf));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException,
                                                                  InterruptedException {

    Cluster.Instance serverInstance = extractAmbariServer(event);

    LOG.info("Authorizing firewall");
    event.getFirewallManager().addRules(
      FirewallManager.Rule.create().destination(serverInstance).ports(AMBARI_SERVER_WEB_UI_PORT));

    ClusterSpec clusterSpec = event.getClusterSpec();

    String configureFunction = getConfiguration(clusterSpec).getString(
      KEY_CONFIGURE_FUNCTION,
      FUNCTION_POST_CONFIGURE);

    addStatement(event, call(RETRY_HELPERS));
    addStatement(event, call(AMBARI_FUNCTIONS));

    addStatement(event, call(configureFunction, AMBARI_SERVER));
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());
    createProxyScript(clusterSpec, cluster);
  }


  @Override
  protected void beforeStart(ClusterActionEvent event) throws IOException, InterruptedException {
    super.beforeStart(event);
    addStatement(event, call(RETRY_HELPERS));
    addStatement(event, call(AMBARI_FUNCTIONS));
    addStatement(event, call(AMBARI_START, AMBARI_SERVER));
  }

  @Override
  protected void afterStart(ClusterActionEvent event) throws IOException, InterruptedException {
    super.afterStart(event);
    Cluster cluster = event.getCluster();
    URL ambariURL = getAmbariServerURL(cluster);
    LOG.info("Ambari web UI available at {}", ambariURL);
    String workerList = createWorkerDescriptionFile(cluster);
    LOG.info("Worker list:\n{}", workerList);
    Configuration conf = getConfiguration(event.getClusterSpec());
    String workerDestFilename = conf.getString(KEY_WORKER_DEST_FILE, null);
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

  @Override
  protected void beforeStop(ClusterActionEvent event) throws IOException, InterruptedException {
    super.beforeStop(event);
    addStatement(event, call(RETRY_HELPERS));
    addStatement(event, call(AMBARI_FUNCTIONS));
    addStatement(event, call(AMBARI_STOP, AMBARI_SERVER));
  }

  /**
   * Get the public URL of the ambari server in this cluster
   * @param cluster cluster
   * @return the externally accessible ambari URL
   * @throws IOException on problems
   * @throws BadDeploymentException if the cluster config isn't right.
   */
  protected URL getAmbariServerURL(Cluster cluster) throws IOException {
    Cluster.Instance instance = cluster.getInstanceMatching(role(AMBARI_SERVER));
    InetAddress masterPublicAddress = instance.getPublicAddress();

    return new URL("http",
                   masterPublicAddress.getHostName(),
                   AMBARI_SERVER_WEB_UI_PORT,
                   AMBARI_SERVER_WEB_UI_PATH);
  }

  /**
   * This creates all the cluster-local IP Addresses for the cluster, but in a NATted infrastructure
   * these are all IP addresses that don't resolve outside the cluster; that don't support rDNS
   * from the Whirr client.
   *
   * These need conversion into a set of hostnames, that can only be done in-cluster
   * @param cluster cluster to work with
   * @return a string of worker nodes, 1 per line, that is only valid inside the cluster.
   * @throws IOException if getting the workers fails.
   */

  protected String createWorkerDescriptionFile(Cluster cluster) throws IOException {

    Cluster.Instance[] workers = getAmbariWorkerArray(cluster);
    int workerCount = workers.length;
    if (workerCount == 0) {
      return null;
    }

    StringBuilder builder = new StringBuilder(workers.length * 64);

    //this loop goes out of its way to avoid leaving a trailing newline at 
    //the end of the the list.
    for (int i = 0; i < workerCount; i++) {
      builder.append(workers[i].getPrivateAddress().getCanonicalHostName());
      if (i < (workerCount - 1)) {
        builder.append("\n");
      }
    }

    return builder.toString();
  }

  private Cluster.Instance[] getAmbariWorkerArray(Cluster cluster) throws IOException {
    Set<Cluster.Instance> workerSet = Utils.getAmbariWorkers(cluster);

    Cluster.Instance[] workers = workerSet.toArray(new Cluster.Instance[workerSet.size()]);
    return workers;
  }


}
