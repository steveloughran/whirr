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

public interface AmbariConstants {


  /**
   * name of server roles in configuration files: {@value}
   */
  String AMBARI_SERVER = "ambari-server";


  /**
   * name of worker role in configuration files: {@value}
   */
  String AMBARI_WORKER = "ambari-worker";
  int AMBARI_SERVER_WEB_UI_PORT = 80;
  String AMBARI_SERVER_WEB_UI_PATH = "/hmc/html/index.php";

  String AMBARI_DEFAULT_PROPERTIES = "whirr-ambari-default.properties";

  String KEY_INSTALL_FUNCTION = "whirr.ambari.install-function";
  String KEY_CONFIGURE_FUNCTION = "whirr.ambari.configure-function";


  String FUNCTION_INSTALL = "install_ambari";
  String FUNCTION_POST_CONFIGURE = "configure_ambari";
  String AMBARI_START = "ambari_start";
  String AMBARI_STOP = "ambari_stop";

  String PROXY_SHELL = "ambari-proxy.sh";

  String AMBARI_FUNCTIONS = "ambari_functions";

  /**
   * This is here to stop checkstyle complaining about an interface with no methods.
   * If CS didn't also complain about wildcarded static imports from a class that 
   * would be used instead.
   */
  String toString();
}
