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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import org.apache.commons.configuration.Configuration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.hdp.BadDeploymentException;
import org.apache.whirr.service.hdp.ClusterProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Set;

import static org.apache.whirr.RolePredicates.role;

public abstract class AbstractAmbariClusterActionHandler extends ClusterActionHandlerSupport {


  private static final Logger LOG =
    LoggerFactory.getLogger(AbstractAmbariClusterActionHandler.class);


  public static InetAddress getAmbariServerPublicAddress(Cluster cluster)
    throws IOException {
    return cluster.getInstanceMatching(
      RolePredicates.role(AmbariConstants.AMBARI_SERVER))
                  .getPublicAddress();
  }

  public static Set<Cluster.Instance> getAmbariWorkers(Cluster cluster)
    throws IOException {
    return cluster.getInstancesMatching(
      RolePredicates.role(AmbariConstants.AMBARI_WORKER));
  }


  /**
   * Returns a composite configuration that is made up from the global
   * configuration coming from the Whirr core with an Ambari defaults properties.
   */
  protected synchronized Configuration getConfiguration(ClusterSpec clusterSpec)
    throws IOException {
    return getConfiguration(clusterSpec, AmbariConstants.AMBARI_DEFAULT_PROPERTIES);
  }

  private File getConfigDir(ClusterSpec clusterSpec) {
    File configDir = new File(new File(System.getProperty("user.home")),
                              ".whirr");
    configDir = new File(configDir, clusterSpec.getClusterName());
    configDir.mkdirs();
    return configDir;
  }

  protected void createProxyScript(ClusterSpec clusterSpec, Cluster cluster) {
    File configDir = getConfigDir(clusterSpec);
    File proxyFile = new File(configDir, AmbariConstants.PROXY_SHELL);
    try {
      ClusterProxy proxy = new ClusterProxy(clusterSpec, cluster);
      InetAddress master = getAmbariServerPublicAddress(cluster);
      String script = String.format(
        "echo 'Running proxy to Ambari cluster at %s. "
        + "Use Ctrl-c to quit.'\n", master.getHostName())
                      + Joiner.on(" ").join(proxy.getProxyCommand(master));
      Files.write(script, proxyFile, Charsets.UTF_8);
      LOG.info("Wrote proxy script {}", proxyFile);
    } catch (IOException e) {
      LOG.error("Problem writing proxy script {}", proxyFile, e);
    }
  }

  @Override
  public void beforeAction(ClusterActionEvent event) throws IOException, InterruptedException {
    LOG.info("[" + getRole() + "]" + " before: " + event.getAction());
    super.beforeAction(event);
  }

  @Override
  public void afterAction(ClusterActionEvent event) throws IOException, InterruptedException {
    LOG.info(getRole() + " after: " + event.getAction());
    super.afterAction(event);
  }

  protected Cluster.Instance extractAmbariServer(ClusterActionEvent event) throws BadDeploymentException {
    Cluster cluster = event.getCluster();

    ClusterSpec clusterSpec = event.getClusterSpec();
    Set<Cluster.Instance> instances = cluster.getInstancesMatching(role(AmbariConstants.AMBARI_SERVER));
    if (instances.isEmpty()) {
      throw new BadDeploymentException("No " + AmbariConstants.AMBARI_SERVER
                                       + " instance in cluster " + cluster.toString()
                                       + " from " + clusterSpec);
    }
    if (instances.size() > 1) {
      throw new BadDeploymentException("More than one " + AmbariConstants.AMBARI_SERVER
                                       + " instance in cluster" + cluster.toString());
    }

    Cluster.Instance instance = instances.iterator().next();

    //verify that the instance isn't also set up to be a worker, as ambari doesn't manage itself.
    if (instance.getRoles().contains(AmbariConstants.AMBARI_WORKER)) {
      throw new BadDeploymentException("The " + AmbariConstants.AMBARI_SERVER
                                       + " instance can not also run an " +
                                       AmbariConstants.AMBARI_WORKER);
    }
    return instance;
  }
}
