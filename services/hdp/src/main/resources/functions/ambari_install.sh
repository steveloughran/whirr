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
set -x


function ambari_install() {

  hdp_register_hortonworks_repo

  echo "about to install epel-release"
  if ! retry_yum -y install epel-release
  then
    echo "Failed to Install epel-release from yum"
    exit 1;
  fi
 
  retry_yum -y update
  local PACKAGES="hmc curl"
  
  echo "about to install $PACKAGES from HDP release $HDP_VERSION"
  retry_yum -y install $PACKAGES
  if ! retry_yum  -y install $PACKAGES
  then
    echo "Yum failed to install $PACKAGES from from HDP release $HDP_VERSION"
    exit 1;
  fi
  INSTALL_AMBARI_DONE=1
}
