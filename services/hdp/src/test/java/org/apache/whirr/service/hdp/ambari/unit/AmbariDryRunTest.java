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

package org.apache.whirr.service.hdp.ambari.unit;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.jcraft.jsch.JSchException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.BaseServiceDryRunTest;
import org.apache.whirr.service.DryRunModule;
import org.apache.whirr.service.hdp.BadDeploymentException;
import org.apache.whirr.service.hdp.ambari.AbstractAmbariClusterActionHandler;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.fail;
import static org.apache.whirr.service.hdp.ambari.AbstractAmbariClusterActionHandler.AMBARI_SERVER;
import static org.apache.whirr.service.hdp.ambari.AbstractAmbariClusterActionHandler.AMBARI_WORKER;

public class AmbariDryRunTest extends BaseServiceDryRunTest {
  private static final Logger LOG =
    LoggerFactory.getLogger(AmbariDryRunTest.class);

  @Override
  protected Set<String> getInstanceRoles() {
    return ImmutableSet.of(AbstractAmbariClusterActionHandler.AMBARI_SERVER);
  }

  @Override
  protected Predicate<CharSequence> configurePredicate() {
    return Predicates.alwaysTrue();
  }

  @Override
  protected Predicate<CharSequence> bootstrapPredicate() {
    return Predicates.alwaysTrue();
  }

  @Override
  protected ClusterSpec newClusterSpecForProperties(Map<String, String> properties) throws
                                                                                    ConfigurationException,
                                                                                    JSchException,

                                                                                    IOException {
    //inject more properties
    Map<String, String> newprops = getTestProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      newprops.put(entry.getKey(), entry.getValue());
    }
    //pass on
    return super.newClusterSpecForProperties(newprops);
  }


  protected Map<String, String> getTestProperties() {
    Map<String, String> props = new HashMap<String, String>();
    return props;
  }

  private DryRunModule.DryRun launchCluster(String template) throws
                                                             ConfigurationException,
                                                             JSchException,
                                                             IOException,
                                                             InterruptedException {
    Map<String, String> props = createInitialProps();
    props.put(
      "whirr.instance-templates", template);
    ClusterSpec cookbookWithDefaultRecipe = newClusterSpecForProperties(props);
    return launchWithClusterSpec(cookbookWithDefaultRecipe);
  }

  private void assertWrapsBadDeploymentException(RuntimeException e) {
    Throwable cause = e.getCause();
    if (cause == null) {
      throw e;
    }
    if (!(BadDeploymentException.class.equals(cause.getClass()))) {
      throw e;
    }
    LOG.info("Ignoring expected exception ", cause);
  }

  @Test
  public void testServerAndMultiWorkerCluster() throws Exception {
    DryRunModule.DryRun dryRun = launchCluster("1 " + AMBARI_SERVER + ",2 " + AMBARI_WORKER);
  }

  @Test
  public void testServerWorkerSameVMForbidden() throws Exception {
    try {
      DryRunModule.DryRun dryRun = launchCluster("1 " + AMBARI_SERVER + "+" + AMBARI_WORKER);
      fail("Expected an error, got a cluster ");
    } catch (RuntimeException e) {
      assertWrapsBadDeploymentException(e);
    }
  }


  @Test
  public void testClusterMustHaveAmbariServer() throws Exception {
    try {
      DryRunModule.DryRun dryRun = launchCluster("1 " + AMBARI_WORKER);
      fail("Expected an error, got a cluster ");
    } catch (RuntimeException e) {
      assertWrapsBadDeploymentException(e);
    }
  }

  @Test
  public void testTwoAmbariServersForbidden() throws Exception {
    try {
      DryRunModule.DryRun dryRun = launchCluster("2 " + AMBARI_SERVER + ",2 " + AMBARI_WORKER);
      fail("Expected an error, got a cluster ");
    } catch (RuntimeException e) {
      assertWrapsBadDeploymentException(e);
    }
  }

  @Test
  public void testKeygenDisabled() throws Exception {
    try {
      Map<String, String> props = createInitialProps();
      props.put(
        "whirr.instance-templates", "1 " + AMBARI_SERVER + ",2 " + AMBARI_WORKER);
      props.put(AbstractAmbariClusterActionHandler.KEY_GENERATE_PUBLIC_KEYS, "false");
      new File("target/ambari_rsa").delete();
      ClusterSpec spec = newClusterSpecForProperties(props);
      DryRunModule.DryRun dryRun = launchWithClusterSpec(spec);
      fail("Expected an error, got a cluster ");
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause == null) {
        throw e;
      }
      Class<? extends Throwable> causeClass = cause.getClass();
      if (!(BadDeploymentException.class.equals(causeClass)) &&
          !FileNotFoundException.class.equals(causeClass)) {
        throw e;
      }
      LOG.info("Ignoring expected exception ", cause);
    }
  }

  private Map<String, String> createInitialProps() {
    Map<String, String> props = new HashMap<String, String>();
    props.put(AbstractAmbariClusterActionHandler.KEY_GENERATE_PUBLIC_KEYS, "true");
    props.put(AbstractAmbariClusterActionHandler.KEY_PRIVATE_KEY_FILE, "target/ambari_rsa");
    return props;
  }
}
