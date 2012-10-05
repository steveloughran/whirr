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

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import org.apache.whirr.Cluster;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.hdp.BadDeploymentException;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Set;

import static org.apache.whirr.RolePredicates.role;

public class Utils {

  protected static final int KEYSIZE = 1024;

  public static InetAddress getAmbariServerPublicAddress(Cluster cluster)
    throws IOException {
    return getAmbariServer(cluster);
  }

  public static InetAddress getAmbariServer(Cluster cluster) throws IOException {
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


  public static String hexify(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    // Send all output to the Appendable object sb
    for (byte b : bytes) {
      String hex = String.format("%02x", b);
      sb.append(hex);
    }
    return sb.toString();
  }

  /**
   * {@link http://www.jcraft.com/jsch/examples/KeyGen.java.html}
   * @return a new keypair
   */
  public static KeyPair createSshKeys() throws JSchException {
    JSch jsch = new JSch();
    KeyPair keyPair = KeyPair.genKeyPair(jsch, KeyPair.RSA);
    return keyPair;
  }

  /**
   * Writes a keypair to file
   *
   * @param keyPair key pair
   * @param privateKeyFile filename to use for the private key; the same filename with .pub at the end becomes
   * the public key.
   * @param comment a comment; use "" for none.
   * @return the public key file
   * @throws IOException any IO problem.
   */
  public static File saveKeyPair(KeyPair keyPair, File privateKeyFile, String comment) throws IOException {
    keyPair.writePrivateKey(privateKeyFile.getAbsolutePath());
    File publicKeyFile = publicKeyPath(privateKeyFile);
    keyPair.writePublicKey(publicKeyFile.getAbsolutePath(), comment);
    //now the private key has to have group read perms removed
    makeOwnerReadAccessOnly(privateKeyFile);
    makeOwnerReadAccessOnly(publicKeyFile);
    return publicKeyFile;
  }

  public static File publicKeyPath(File privateKeyFile) {
    return new File(privateKeyFile.getAbsolutePath() + ".pub");
  }

  public static void makeOwnerReadAccessOnly(File file) {
    //non readable by all
    file.setReadable(false, false);
    //readable by root alone
    file.setReadable(true, true);
  }

  /**
   * This creates all the cluster-local IP Addresses for the cluster, but in a NATted infrastructure
   * these are all IP addresses that don't resolve outside the cluster; that don't support rDNS
   * from the Whirr client.
   *
   * These need conversion into a set of hostnames, that can only be done in-cluster
   *
   * @param cluster cluster to work with
   * @param domain the domain, which must be "" or a domain string with a leading "."
   * @return a string of worker nodes, 1 per line, that is only valid inside the cluster.
   * @throws IOException if getting the workers fails.
   */

  public static String createWorkerDescriptionFile(Cluster cluster, String domain) throws IOException {

    Cluster.Instance[] workers = getAmbariWorkerArray(cluster);
    int workerCount = workers.length;
    if (workerCount == 0) {
      return null;
    }

    StringBuilder builder = new StringBuilder(workers.length * 64);

    //this loop goes out of its way to avoid leaving a trailing newline at 
    //the end of the the list.
    for (int i = 0; i < workerCount; i++) {
      builder.append(workers[i].getPrivateAddress().getHostName());
      builder.append(domain);
      if (i < (workerCount - 1)) {
        builder.append("\n");
      }
    }

    return builder.toString();
  }

  public static Cluster.Instance[] getAmbariWorkerArray(Cluster cluster) throws IOException {
    Set<Cluster.Instance> workerSet = getAmbariWorkers(cluster);

    return workerSet.toArray(new Cluster.Instance[workerSet.size()]);
  }
}
