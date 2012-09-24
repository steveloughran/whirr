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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.jcraft.jsch.JSchException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.BaseServiceDryRunTest;
import org.apache.whirr.service.DryRunModule;
import static org.apache.whirr.service.hdp.ambari.AmbariConstants.*;
import static junit.framework.Assert.*;

import org.apache.whirr.service.hdp.BadDeploymentException;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AmbariDryRunTest extends BaseServiceDryRunTest {

  @Override
  protected Set<String> getInstanceRoles() {
    return ImmutableSet.of(AMBARI_SERVER);
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

  private DryRunModule.DryRun launchCluster(String template) throws ConfigurationException, JSchException, IOException, InterruptedException {
    ClusterSpec cookbookWithDefaultRecipe = newClusterSpecForProperties(ImmutableMap.of(
      "whirr.instance-templates", template));
    return launchWithClusterSpec(cookbookWithDefaultRecipe);
  }

  /**
   * Tests that a simple cluster is correctly loaded and executed.
   */
  @Test
  public void testServerWorkerSameVMForbidden() throws Exception {
    try {
      DryRunModule.DryRun dryRun = launchCluster("1 " + AMBARI_SERVER + "+" + AMBARI_WORKER);
      fail("Expected an error, got a cluster ");
    } catch (RuntimeException e) {
      assertWrapsBadDeploymentException(e);
    }
  }

  private void assertWrapsBadDeploymentException(RuntimeException e) {
    Throwable cause = e.getCause();
    assertNotNull(cause);
    assertEquals(BadDeploymentException.class,cause.getClass());
  }

  /**
   * Tests that a simple cluster is correctly loaded and executed.
   */
  @Test
  public void testClusterMustHaveAmbariServer() throws Exception {
    try {
      DryRunModule.DryRun dryRun = launchCluster("1 "+ AMBARI_WORKER);
      fail("Expected an error, got a cluster ");
    } catch (RuntimeException e) {
      assertWrapsBadDeploymentException(e);
    }
  }

}
