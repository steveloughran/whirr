#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
function ambari_configure() {
  if [ "${CONFIGURE_AMBARI_DONE}" == "1" ]; then
    echo "Ambari is already configured."
    return;
  fi
  
  local ROLES=$1
  shift
  
  echo "Roles=$ROLES"
  if [ $(echo "$ROLES" | grep "ambari-server" | wc -l) -gt 0 ]; then
    stop_iptables
    start_ambari_daemon hmc yes
  fi
  
  CONFIGURE_AMBARI_DONE=1
  
}

# turn IP tables off. As this point we rely on the infrastructure's network rules to 
# keep the box secure
function stop_iptables() {
  echo "Stopping iptables"
  service iptables stop
}

#Start the daemon -failure to do so triggers a script failure
function start_ambari_daemon() {
  local daemon
  local retval
  daemon=$1
  local command=$2
  echo "Starting $daemon"
  echo command | service $daemon start
  retval=$?
  if ((${retval} == 0))
  then
    echo "Service $daemon is started"
  else
    echo "Service $daemon failed with return code ${retval}"
    exit 1;
  fi
  
#  now do a service status check to verify the service is live
  echo "pinging $daemon"
  service $daemon status
  retval=$?
  if ((${retval} == 0))
  then
    echo "Service $daemon is started"
  else
    echo "Service $daemon failed with return code ${retval}"
    exit 1;
  fi
  
  ##and for a final sanity check, curl the URL
  
  curl --location --max-time 120 http://localhost/hmc/html/
}

