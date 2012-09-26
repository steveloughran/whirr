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
function ambari_start() {
  if [ "${START_AMBARI_DONE}" == "1" ]
  then
    echo "Ambari is already started."
    return;
  fi
  
  local ROLES=$1
  shift
  
  echo "Roles=$ROLES"
  if [ $(echo "$ROLES" | grep "ambari-server" | wc -l) -gt 0 ]
  then
    start_ambari_daemon hmc
  fi
  
  START_AMBARI_DONE=1
  
}


