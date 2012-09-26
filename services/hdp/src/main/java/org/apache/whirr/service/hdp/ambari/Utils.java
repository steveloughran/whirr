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
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.hdp.BadDeploymentException;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Set;

import static org.apache.whirr.RolePredicates.role;

public class Utils {
  public static InetAddress getAmbariServerPublicAddress(Cluster cluster)
    throws IOException {
    return cluster.getInstanceMatching(
      RolePredicates.role(AbstractAmbariClusterActionHandler.AMBARI_SERVER))
                  .getPublicAddress();
  }

  public static Set<Cluster.Instance> getAmbariWorkers(Cluster cluster)
    throws IOException {
    return cluster.getInstancesMatching(
      RolePredicates.role(AbstractAmbariClusterActionHandler.AMBARI_WORKER));
  }

  /**
   * Extract the single server in a cluster. This verifies that there is exactly one such
   * server, and that it is not also a worker
   * @param role role to look for
   * @param cluster cluster to look in
   * @return the single instance 
   * @throws BadDeploymentException on any problem
   */
  public static Cluster.Instance locateSingleServerInstance(String role, Cluster cluster) throws BadDeploymentException {
    if (cluster.getInstances() == null) {
      throw new BadDeploymentException("Server location cannot be performed until the cluster is instantiated");
    }
    Set<Cluster.Instance> instances = cluster.getInstancesMatching(role(role));
    if (instances.isEmpty()) {
      throw new BadDeploymentException("No " + role
                                       + " instance in cluster " + cluster.toString());
    }
    if (instances.size() > 1) {
      throw new BadDeploymentException("More than one " + role
                                       + " instance in cluster" + cluster.toString());
    }

    return instances.iterator().next();
  }
}
