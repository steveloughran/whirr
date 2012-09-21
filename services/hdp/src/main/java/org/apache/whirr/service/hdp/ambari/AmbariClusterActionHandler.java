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
import org.apache.whirr.service.FirewallManager;
import org.apache.whirr.service.hadoop.HadoopProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;

import static org.apache.whirr.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

public class AmbariClusterActionHandler extends
  ClusterActionHandlerSupport {

  private static final Logger LOG =
    LoggerFactory.getLogger(AmbariClusterActionHandler.class);


  @Override
  public String getRole() {
    return AmbariConstants.ROLE;
  }

  /**
   * Returns a composite configuration that is made up from the global
   * configuration coming from the Whirr core with an Ambari defaults properties.
   */
  protected synchronized Configuration getConfiguration(ClusterSpec clusterSpec)
    throws IOException {
    return getConfiguration(clusterSpec, AmbariConstants.AMBARI_DEFAULT_PROPERTIES);
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException,
                                                                  InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Authorizing firewall");
    Set<Cluster.Instance> instances = cluster.getInstancesMatching(role(AmbariConstants.ROLE));
    if (instances.isEmpty()) {
      throw new BadDeploymentException("No " + AmbariConstants.ROLE + " instance in cluster");
    }
    if (instances.size() > 1) {
      throw new BadDeploymentException("More than one " + AmbariConstants.ROLE + " instance in cluster");
    }

    Cluster.Instance instance = instances.iterator().next();

    event.getFirewallManager().addRules(
      FirewallManager.Rule.create().destination(instance).ports(AmbariConstants.AMBARI_SERVER_WEB_UI_PORT));

    String configureFunction = getConfiguration(clusterSpec).getString(
      AmbariConstants.KEY_CONFIGURE_FUNCTION,
      AmbariConstants.FUNCTION_POST_CONFIGURE);

    addStatement(event, call("retry_helpers"));

    addStatement(event, call(configureFunction, AmbariConstants.ROLE));

  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());
    Cluster.Instance instance = cluster.getInstanceMatching(role(AmbariConstants.ROLE));
    InetAddress masterPublicAddress = instance.getPublicAddress();

    LOG.info("Ambari web UI available at http://{}:{}",
             masterPublicAddress.getHostName(),
             AmbariConstants.AMBARI_SERVER_WEB_UI_PORT);

    Properties config = new Properties();
    createProxyScript(clusterSpec, cluster);
    event.setCluster(new Cluster(cluster.getInstances(), config));
  }

  private File getConfigDir(ClusterSpec clusterSpec) {
    File configDir = new File(new File(System.getProperty("user.home")),
                              ".whirr");
    configDir = new File(configDir, clusterSpec.getClusterName());
    configDir.mkdirs();
    return configDir;
  }


  private void createProxyScript(ClusterSpec clusterSpec, Cluster cluster) {
    File configDir = getConfigDir(clusterSpec);
    File proxyFile = new File(configDir, AmbariConstants.PROXY_SHELL);
    try {
      HadoopProxy proxy = new HadoopProxy(clusterSpec, cluster);
      InetAddress master = getAmbariServerPublicAddress(cluster);
      String script = String.format(
        "echo 'Running proxy to Ambari cluster at %s. "
        + "Use Ctrl-c to quit.'\n", master.getHostName())
                      + Joiner.on(" ").join(proxy.getProxyCommand());
      Files.write(script, proxyFile, Charsets.UTF_8);
      LOG.info("Wrote proxy script {}", proxyFile);
    } catch (IOException e) {
      LOG.error("Problem writing proxy script {}", proxyFile, e);
    }
  }


  public static InetAddress getAmbariServerPublicAddress(Cluster cluster)
    throws IOException {
    return cluster.getInstanceMatching(
      RolePredicates.role(AmbariConstants.ROLE))
                  .getPublicAddress();
  }
}
