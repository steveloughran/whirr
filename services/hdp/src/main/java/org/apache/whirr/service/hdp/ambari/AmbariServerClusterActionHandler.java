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
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;

import static org.apache.whirr.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

public class AmbariServerClusterActionHandler extends AbstractAmbariClusterActionHandler {

  private static final Logger LOG =
    LoggerFactory.getLogger(AmbariServerClusterActionHandler.class);


  @Override
  public String getRole() {
    return AmbariConstants.AMBARI_SERVER;
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException,
                                                                  InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Authorizing firewall");
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

    event.getFirewallManager().addRules(
      FirewallManager.Rule.create().destination(instance).ports(AmbariConstants.AMBARI_SERVER_WEB_UI_PORT));

    String installFunction = getConfiguration(clusterSpec).getString(
      AmbariConstants.KEY_INSTALL_FUNCTION,
      AmbariConstants.FUNCTION_INSTALL);
    String configureFunction = getConfiguration(clusterSpec).getString(
      AmbariConstants.KEY_CONFIGURE_FUNCTION,
      AmbariConstants.FUNCTION_POST_CONFIGURE);

    addStatement(event, call("retry_helpers"));

    addStatement(event, call(installFunction, AmbariConstants.AMBARI_SERVER));
    addStatement(event, call(configureFunction, AmbariConstants.AMBARI_SERVER));

  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());
    Cluster.Instance instance = cluster.getInstanceMatching(role(AmbariConstants.AMBARI_SERVER));
    InetAddress masterPublicAddress = instance.getPublicAddress();

    LOG.info("Ambari web UI available at http://{}:{}",
             masterPublicAddress.getHostName(),
             AmbariConstants.AMBARI_SERVER_WEB_UI_PORT);

    Properties config = new Properties();
    createProxyScript(clusterSpec, cluster);
    event.setCluster(new Cluster(cluster.getInstances(), config));
  }


}
