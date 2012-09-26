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
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.hdp.BadDeploymentException;
import org.jclouds.scriptbuilder.statements.ssh.AuthorizeRSAPublicKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public final class AmbariWorkerClusterActionHandler extends AbstractAmbariClusterActionHandler {


  private static final Logger LOG =
    LoggerFactory.getLogger(AmbariWorkerClusterActionHandler.class);
  private File pubKeyFile;
  private String workerPubKey;


  @Override
  public String getRole() {
    return AMBARI_WORKER;
  }


  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException, InterruptedException {
    Configuration conf = getConfiguration(event);
    pubKeyFile = getFile(conf, KEY_PUBLIC_KEY_FILE);
    if (pubKeyFile == null) {
      throw new BadDeploymentException("No public key for Ambari Workers defined in " + KEY_PUBLIC_KEY_FILE);
    }
    workerPubKey = FileUtils.readFileToString(pubKeyFile, "US-ASCII");

  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
//    Cluster.Instance serverInstance = extractAmbariServer(event);

    List<String> keys= new LinkedList<String>();
    keys.add(workerPubKey);
    
    
    AuthorizeRSAPublicKeys authKeys= new AuthorizeRSAPublicKeys(keys);
      
    addStatement(event,authKeys);

//    File privateKey = getFile(conf, KEY_PRIVATE_KEY_FILE);


  }

  @Override
  protected void afterStart(ClusterActionEvent event) throws IOException, InterruptedException {

  }

  protected File getFile(Configuration conf, String key) {
    String filename = conf.getString(key, null);
    if (filename == null) {
      return null;
    } else {
      return new File(filename);
    }
  }
}
