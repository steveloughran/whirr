/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.jclouds;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

class VariablesToExport implements Supplier<Map<String, String>> {

  private static final Logger LOG =
    LoggerFactory.getLogger(StatementBuilder.class);
   
  private final Map<String, String> exports;
  private final Map<String, Map<String, String>> exportsByInstanceId;
  private final ClusterSpec clusterSpec;
  private final Instance instance;

  public VariablesToExport(Map<String, String> exports, Map<String, Map<String, String>> exportsByInstanceId,
      ClusterSpec clusterSpec, Instance instance) {
    this.exports = ImmutableMap.copyOf(exports);
    this.exportsByInstanceId = ImmutableMap.copyOf(exportsByInstanceId);
    this.clusterSpec = clusterSpec;
    this.instance = instance;
  }

  @Override
  public Map<String, String> get() {
    Map<String, String> metadataMap = Maps.newLinkedHashMap();

    addEnvironmentVariablesFromClusterSpec(metadataMap);
    addDefaultEnvironmentVariablesForInstance(metadataMap, instance);
    metadataMap.putAll(exports);
    addPerInstanceCustomEnvironmentVariables(metadataMap, instance);

    return metadataMap;
  }

  private void addPerInstanceCustomEnvironmentVariables(Map<String, String> metadataMap, Instance instance) {
    if (instance != null && exportsByInstanceId.containsKey(instance.getId())) {
      metadataMap.putAll(exportsByInstanceId.get(instance.getId()));
    }
  }

  private void addDefaultEnvironmentVariablesForInstance(Map<String, String> metadataMap, Instance instance) {
    if (clusterSpec.getClusterName() != null)
      metadataMap.put("CLUSTER_NAME", clusterSpec.getClusterName());
    if (clusterSpec.getClusterUser() != null)
      metadataMap.put("CLUSTER_USER", clusterSpec.getClusterUser());
    if (clusterSpec.getProvider() != null)
      metadataMap.put("CLOUD_PROVIDER", clusterSpec.getProvider());
    if (clusterSpec.getAutoHostnameSuffix() != null
        && !clusterSpec.getAutoHostnameSuffix().equals("")) {
        metadataMap.put("AUTO_HOSTNAME_SUFFIX", clusterSpec.getAutoHostnameSuffix());
        if (clusterSpec.getAutoHostnamePrefix() != null
            && !clusterSpec.getAutoHostnamePrefix().equals("")) {
            metadataMap.put("AUTO_HOSTNAME_PREFIX", clusterSpec.getAutoHostnamePrefix());
        }
    }
    if(clusterSpec.getJdkInstallUrl() != null) {
      metadataMap.put("JDK_INSTALL_URL", clusterSpec.getJdkInstallUrl());
    }
    metadataMap.put("KERBEROS_REALM",
      clusterSpec.getKerberosRealm() == null ? "HADOOPCLUSTER.COM" : clusterSpec.getKerberosRealm());
    if (instance != null) {
      metadataMap.put("ROLES", Joiner.on(",").join(instance.getRoles()));
      if (instance.getPublicIp() != null)
        metadataMap.put("PUBLIC_IP", instance.getPublicIp());
      if (instance.getPrivateIp() != null)
         metadataMap.put("PRIVATE_IP", instance.getPrivateIp());
      if (!clusterSpec.isStub()) {
        try {
          if (instance.getPublicIp() != null)
            metadataMap.put("PUBLIC_HOST_NAME", instance.getPublicHostName());
          if (instance.getPrivateIp() != null)
            metadataMap.put("PRIVATE_HOST_NAME", instance.getPrivateHostName());
        } catch (IOException e) {
          LOG.warn("Could not resolve hostname for " + instance, e);
        }
      }
    }
  }

  private void addEnvironmentVariablesFromClusterSpec(Map<String, String> metadataMap) {
    for (Iterator<?> it = clusterSpec.getConfiguration().getKeys("whirr.env"); it.hasNext(); ) {
      String key = (String) it.next();
      String value = clusterSpec.getConfiguration().getString(key);
      metadataMap.put(key.substring("whirr.env.".length()).toUpperCase(Locale.US), value);
    }
  }
}
