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
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.hdp.BadDeploymentException;
import org.apache.whirr.service.hdp.ClusterProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;

public abstract class AbstractAmbariClusterActionHandler extends ClusterActionHandlerSupport {


  public static final String RETRY_HELPERS = "retry_helpers";
  /**
   * name of server roles in configuration files: {@value}
   */
  public static final String AMBARI_SERVER = "ambari-server";


  /**
   * name of worker role in configuration files: {@value}
   */
  public static final String AMBARI_WORKER = "ambari-worker";
  public static final int AMBARI_SERVER_WEB_UI_PORT = 80;
  public static final String AMBARI_SERVER_WEB_UI_PATH = "/hmc/html/index.php";

  public static final String AMBARI_DEFAULT_PROPERTIES = "whirr-ambari-default.properties";

  public static final String KEY_INSTALL_FUNCTION = "whirr.ambari.install-function";
  public static final String KEY_CONFIGURE_FUNCTION = "whirr.ambari.configure-function";
  public static final String KEY_PRIVATE_KEY_FILE = "whirr.ambari.private-key-file";
  public static final String KEY_PUBLIC_KEY_FILE = "whirr.ambari.public-key-file";
  /**
   * If this is set, the keys are generated. If not, the key files must exist
   */
  public static final String KEY_GENERATE_PUBLIC_KEYS = "whirr.ambari.generate-public-keys";
  
  public static final String KEY_WORKER_DEST_FILE = "whirr.ambari.worker-list-file";


  public static final String FUNCTION_INSTALL = "install_ambari";
  public static final String FUNCTION_POST_CONFIGURE = "configure_ambari";
  public static final String AMBARI_START = "ambari_start";
  public static final String AMBARI_STOP = "ambari_stop";

  public static final String PROXY_SHELL = "ambari-proxy.sh";

  public static final String AMBARI_FUNCTIONS = "ambari_functions";


  private static final Logger LOG =
    LoggerFactory.getLogger(AbstractAmbariClusterActionHandler.class);


  /**
   * Returns a composite configuration that is made up from the global
   * configuration coming from the Whirr core with an Ambari defaults properties.
   */
  protected synchronized Configuration getConfiguration(ClusterSpec clusterSpec)
    throws IOException {
    return getConfiguration(clusterSpec, AMBARI_DEFAULT_PROPERTIES);
  }

  protected synchronized Configuration getConfiguration(ClusterActionEvent event)
    throws IOException {
    return getConfiguration(event.getClusterSpec());
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
    File proxyFile = new File(configDir, PROXY_SHELL);
    try {
      ClusterProxy proxy = new ClusterProxy(clusterSpec, cluster);
      InetAddress master = Utils.getAmbariServerPublicAddress(cluster);
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

  /**
   * Extract the single ambari server in a cluster. This verifies that there is exactly one such
   * server, and that it is not also a worker
   * @param event the event containing the cluster & its spec
   * @return the single instance 
   * @throws BadDeploymentException on any problem
   */
  protected Cluster.Instance extractAmbariServer(ClusterActionEvent event) throws BadDeploymentException {
    Cluster.Instance instance = locateSingleServerInstance(event, AMBARI_SERVER);

    //verify that the instance isn't also set up to be a worker, as ambari doesn't manage itself.
    if (instance.getRoles().contains(AMBARI_WORKER)) {
      throw new BadDeploymentException("The " + AMBARI_SERVER
                                       + " instance can not also run an " +
                                       AMBARI_WORKER);
    }
    return instance;
  }

  /**
   * Extract the single server in a cluster. This verifies that there is exactly one such
   * server, and that it is not also a worker
   * @param event the event containing the cluster & its spec
   * @param role role to look for
   * @return the single instance 
   * @throws BadDeploymentException on any problem
   */

  public static Cluster.Instance locateSingleServerInstance(ClusterActionEvent event, String role) throws BadDeploymentException {
    Cluster cluster = event.getCluster();
    return Utils.locateSingleServerInstance(role, cluster);
  }


  public File bindToExistingFile(String key, String path) throws FileNotFoundException {
    File file= new File(path);
    if (!file.exists()) {
      throw new FileNotFoundException(
        String.format("File referenced by key %s not found %s", key, file.getAbsolutePath()));
    }
    return file;
  }

  /**
   * read and validate ssh key options
   * @param conf configuration
   * @return true if the keys are to be generated
   * @throws IOException
   */
  public boolean validateSSHKeyOptions(Configuration conf) throws IOException {
    boolean generate = conf.getBoolean(KEY_GENERATE_PUBLIC_KEYS, false);
    if (!generate) {
      String privateKeyFile = conf.getString(KEY_PRIVATE_KEY_FILE, "");
      bindToExistingFile(KEY_PRIVATE_KEY_FILE, privateKeyFile);
      String publicKeyFile = conf.getString(KEY_PUBLIC_KEY_FILE, "");
      bindToExistingFile(KEY_PUBLIC_KEY_FILE, publicKeyFile);
    } else {
      throw new BadDeploymentException("Keygen not yet working");
    }
    return generate;
  }
    public Configuration createOrValidateKeys(Configuration conf) throws IOException {
    if (validateSSHKeyOptions(conf)) {
      return createAndAddKeys(conf);
    } else {
      return conf;
    }
  }
  
  public String loadPublicKey(Configuration conf) throws IOException {
    String publicKeyFile = conf.getString(KEY_PUBLIC_KEY_FILE, "");
    File file = bindToExistingFile(KEY_PUBLIC_KEY_FILE, publicKeyFile);
    String s = FileUtils.readFileToString(file);
    if (s==null || s.isEmpty()) {
      throw new BadDeploymentException(String.format("The contents of the file %s are empty, not a public key", file));
    }
    return s;
  }


  /**
   * Create a keypair and add it to the configuration. 
   * 
   * @param conf
   * @return
   * @throws IOException
   */
  public Configuration createAndAddKeys(Configuration conf) throws IOException {
    try {
      String destFilename = conf.getString(KEY_PRIVATE_KEY_FILE, "");

      File keyFile;
      if (!destFilename.isEmpty()) {
        keyFile = new File(destFilename);
      } else {
        keyFile = File.createTempFile("ssh", "-key");
      }
      //absolutize
      String keyFileAbsolutePath = keyFile.getAbsolutePath();
      conf.setProperty(KEY_PRIVATE_KEY_FILE, keyFileAbsolutePath);
      KeyPair sshKeys = Utils.createSshKeys();
      LOG.debug("saving to {}", keyFileAbsolutePath);
      Utils.saveKeyPair(sshKeys, keyFile, "ambari-generated");
      File pubFile = Utils.publicKeyPath(keyFile);
      conf.setProperty(KEY_PUBLIC_KEY_FILE, pubFile.getAbsolutePath());
      return conf;
    } catch (JSchException e) {
      throw new IOException("Failed to create Ambari public Keys "+e,e);
    }
  }

}