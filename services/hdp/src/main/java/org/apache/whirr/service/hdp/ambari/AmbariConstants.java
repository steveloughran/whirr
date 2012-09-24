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

public class AmbariConstants {


  /**
   * name of server roles in configuration files: {@value}
   */
  public static final String AMBARI_SERVER = "ambari-server";


  /**
   * name of worker role in configuration files: {@value}
   */
  public static final String AMBARI_WORKER = "ambari-worker";
  public static final int AMBARI_SERVER_WEB_UI_PORT = 80;

  public static final String AMBARI_DEFAULT_PROPERTIES = "whirr-ambari-default.properties";

  public static final String KEY_INSTALL_FUNCTION = "whirr.ambari.install-function";
  public static final String KEY_CONFIGURE_FUNCTION = "whirr.ambari.configure-function";


  public static final String KEY_TARBALL_URL = "whirr.ambari.tarball.url";
  public static final String KEY_START_FUNCTION = "whirr.ambari.start-function";

  public static final String FUNCTION_INSTALL = "install_ambari";
  public static final String FUNCTION_POST_CONFIGURE = "configure_ambari";
  public static final String FUNCTION_START = "start_ambari";

  public static final String PROXY_SHELL = "ambari-proxy.sh";

}
