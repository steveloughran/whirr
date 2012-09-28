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

import com.jcraft.jsch.KeyPair;
import org.apache.commons.io.IOUtils;
import org.apache.whirr.service.hdp.ambari.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class KeyPairTest extends Assert {


  private static final Logger LOG =
    LoggerFactory.getLogger(KeyPairTest.class);

  @Test
  public void testKenGeyPair() throws Throwable {
    KeyPair sshKeys = Utils.createSshKeys();
    LOG.info("public key {}", Utils.hexify(sshKeys.getPublicKeyBlob()));
  }

  @Test
  public void testWriteKeyPair() throws Throwable {
    File keyfile = new File("target/sshkey");
    KeyPair sshKeys = Utils.createSshKeys();
    LOG.info("saving to {}", keyfile.getAbsolutePath());
    File pubKeyFile = Utils.saveKeyPair(sshKeys, keyfile, "keypairtest-generated");
    assertFileUserReadOnly(keyfile);
    assertFileUserReadOnly(pubKeyFile);
  }


  @Test
  public void testReadOnlySettings() throws Throwable {
    File file = File.createTempFile("foo", "txt");
    try {
      Utils.makeOwnerReadAccessOnly(file);
      boolean readable = file.canRead();
      assertTrue(readable);
      assertFileUserReadOnly(file);
    } finally {
      file.delete();
    }
  }

  private void assertFileUserReadOnly(File file) throws IOException, InterruptedException {
    Process process = Runtime.getRuntime().exec(new String[]{
      "ls", "-l", file.getAbsolutePath()
    });
    int retval = process.waitFor();
    InputStream stdoutStr = process.getInputStream();
    String stdout = IOUtils.toString(stdoutStr);
    String stderr = IOUtils.toString(process.getErrorStream());
    LOG.info("stdout={}\nstderr={}", stdout, stderr);
    assertEquals("ps failure, stderr=" + stderr, 0, retval);
    assertTrue("wrong perms in " + stdout, stdout.contains("-rw-------"));
  }
}
