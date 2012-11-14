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

package org.apache.whirr;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.whirr.util.KeyPair.sameKeyPair;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.whirr.internal.ConfigToTemplateBuilderSpec;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.jclouds.javax.annotation.Nullable;
import org.jclouds.predicates.validators.DnsNameValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;

/**
 * This class represents the specification of a cluster. It is used to describe
 * the properties of a cluster before it is launched.
 */
public class ClusterSpec {
  
  static final Logger LOG = LoggerFactory.getLogger(ClusterSpec.class);

  public enum Property {
    CLUSTER_NAME(String.class, false,  "The name of the cluster " +
      "to operate on. E.g. hadoopcluster."),

    SERVICE_NAME(String.class, false, "(optional) The name of the " +
      "service to use. E.g. hadoop."),

    CLUSTER_USER(String.class, false, "The name of the user that Whirr " +
            "will create on all the cluster instances. You have to use " +
            "this user to login to nodes."),

    INSTANCE_TEMPLATES(String.class, false, "The number of instances " +
      "to launch for each set of roles. E.g. 1 hadoop-namenode+" +
      "hadoop-jobtracker, 10 hadoop-datanode+hadoop-tasktracker"),
      
    INSTANCE_TEMPLATES_MAX_PERCENT_FAILURES(String.class, false, "The percentage " +
      "of successfully started instances for each set of roles. E.g. " + 
      "100 hadoop-namenode+hadoop-jobtracker,60 hadoop-datanode+hadoop-tasktracker means " + 
      "all instances with the roles hadoop-namenode and hadoop-jobtracker " + 
      "has to be successfully started, and 60% of instances has to be succcessfully " + 
      "started each with the roles hadoop-datanode and hadoop-tasktracker."),

    INSTANCE_TEMPLATES_MINIMUM_NUMBER_OF_INSTANCES(String.class, false, "The minimum number" +
      "of successfully started instances for each set of roles. E.g. " +
      "1 hadoop-namenode+hadoop-jobtracker,6 hadoop-datanode+hadoop-tasktracker means " + 
      "1 instance with the roles hadoop-namenode and hadoop-jobtracker has to be successfully started," +
      " and 6 instances has to be successfully started each with the roles hadoop-datanode and hadoop-tasktracker."),

    MAX_STARTUP_RETRIES(Integer.class, false, "The number of retries in case of insufficient " + 
        "successfully started instances. Default value is 1."),
    
    PROVIDER(String.class, false, "The name of the cloud provider. " + 
      "E.g. aws-ec2, cloudservers-uk"),

    ENDPOINT(String.class, false, "optionally specifies the url of the " +
      "compute provider.  For example, for openstack-nova, it is the " +
      "keystone url, like: http://localhost:5000/v2.0/."),
      
    IDENTITY(String.class, false, "The cloud identity."),
      
    CREDENTIAL(String.class, false, "The cloud credential."),

    PUBLIC_KEY_FILE(String.class, false, "The filename of the public " +
      "key used to connect to instances."),
      
    PRIVATE_KEY_FILE(String.class, false, "The filename of the " + 
      "private RSA key used to connect to instances."),

    BLOBSTORE_PROVIDER(String.class, false, "The blob store provider. " +
      "E.g. aws-s3, cloudfiles-us, cloudfiles-uk"),

    BLOBSTORE_ENDPOINT(String.class, false, "The blob store endpoint"),

    BLOBSTORE_IDENTITY(String.class, false, "The blob store identity"),

    BLOBSTORE_CREDENTIAL(String.class, false, "The blob store credential"),

    BLOBSTORE_LOCATION_ID(String.class, false, "The blob store location ID"),

    BLOBSTORE_CACHE_CONTAINER(String.class, false, "The name of the " +
        "container to be used for caching local files. If not specified Whirr will " +
        "create a random one and remove it at the end of the session."),

    STATE_STORE(String.class, false, "What kind of store to use for state " +
      "(local, blob or none). Defaults to local."),

    STATE_STORE_CONTAINER(String.class, false, "Container where to store state. " +
      "Valid only for the blob state store."),

    STATE_STORE_BLOB(String.class, false, "Blob name for state storage. " +
      "Valid only for the blob state store. Defaults to whirr-<cluster-name>"),

    AWS_EC2_SPOT_PRICE(Float.class, false, "Spot instance price (aws-ec2 specific option)"),
    
    TEMPLATE(String.class, true, "The specification of requirements for instances in jclouds "+
       "TemplateBuilderSpec format.  default. \"osFamily=UBUNTU,osVersionMatches=10.04,minRam=1024\". "+
       "Note that this is a an alternative not compatible with the following properties: "+
       "image-id, hardware-id, location-id, hardware-min-ram, bootstrap-user."),

    BOOTSTRAP_USER(String.class, false,  "Override the default login user "+
      "used to bootstrap whirr. E.g. ubuntu or myuser:mypass."),
       
    IMAGE_ID(String.class, false, "The ID of the image to use for " + 
      "instances. If not specified then a vanilla Linux image is " + 
      "chosen."),
      
    HARDWARE_ID(String.class, false, "The type of hardware to use for" + 
      " the instance. This must be compatible with the image ID."),

    HARDWARE_MIN_RAM(Integer.class, false, "The minimum amount of " +
      "instance memory. E.g. 1024"),

    LOCATION_ID(String.class, false, "The location to launch " + 
      "instances in. If not specified then an arbitrary location " + 
      "will be chosen."),

    CLIENT_CIDRS(String.class, true, "A comma-separated list of CIDR" +
      " blocks. E.g. 208.128.0.0/11,108.128.0.0/11"),
      
    FIREWALL_RULES(String.class, true, "A comma-separated list of port" +
      " numbers. E.g. 8080,8181"),
          
    FIREWALL_RULES_ROLE(String.class, true, "A comma-separated list of port" +
      " numbers. E.g. 8080,8181. Replace 'role' with an actual role name"),
        
    VERSION(String.class, false, ""),
    
    RUN_URL_BASE(String.class, false, "The base URL for forming run " + 
      "urls from. Change this to host your own set of launch scripts."),
      
    TERMINATE_ALL_ON_LAUNCH_FAILURE(Boolean.class, false, "Whether or not to " +
                                    "automatically terminate all nodes when cluster launch fails for some reason."),

    STORE_CLUSTER_IN_ETC_HOSTS(Boolean.class, false, "Whether or not to " +
                               "store all cluster IPs and hostnames in /etc/hosts on each node."),

    AUTO_HOSTNAME_PREFIX(String.class, false, "If given, used a prefix when automatically " +
                         "generating hostnames. Ignored if AUTO_HOSTNAME_SUFFIX is not also set."),

    AUTO_HOSTNAME_SUFFIX(String.class, false, "If given, use this (along with AUTO_HOSTNAME_PREFIX, if set) to set " +
                             "the hostname for the instances."),

    JDK_INSTALL_URL(String.class, false, "JDK install URL"),
    
    KERBEROS_REALM(String.class, false, "Kerberos realm to use in security configuration"),
                             
    AWS_EC2_PLACEMENT_GROUP(String.class, false, "If given, use this existing EC2 placement group. (aws-ec2 specific option)");
    
    private Class<?> type;
    private boolean multipleArguments;
    private String description;
    
    Property(Class<?> type, boolean multipleArguments, String description) {
      this.type = type;
      this.multipleArguments = multipleArguments;
      this.description = description;
    }
    
    public String getSimpleName() {
      return name().toLowerCase().replace('_', '-');
    }

    public String getConfigName() {
      return "whirr." + getSimpleName();
    }
    
    public Class<?> getType() {
      return type;
    }
    
    public boolean hasMultipleArguments() {
      return multipleArguments;
    }
    
    public String getDescription() {
      return description;
    }
  }

  private static final String DEFAULT_PROPERTIES = "whirr-default.properties";

  /**
   * Create an instance that uses a temporary RSA key pair.
   */
  @VisibleForTesting
  public static ClusterSpec withTemporaryKeys()
  throws ConfigurationException, JSchException, IOException {
    return withTemporaryKeys(new PropertiesConfiguration());
  }
  
  @VisibleForTesting
  public static ClusterSpec withTemporaryKeys(Configuration conf)
  throws ConfigurationException, JSchException, IOException {
    if (!conf.containsKey(Property.PRIVATE_KEY_FILE.getConfigName())) {
      Map<String, File> keys = org.apache.whirr.util.KeyPair.generateTemporaryFiles();

      LoggerFactory.getLogger(ClusterSpec.class).debug("ssh keys: " +
            keys.toString());
      
      conf.addProperty(Property.PRIVATE_KEY_FILE.getConfigName(), 
        keys.get("private").getAbsolutePath());
      conf.addProperty(Property.PUBLIC_KEY_FILE.getConfigName(), 
        keys.get("public").getAbsolutePath());
    }
    
    return new ClusterSpec(conf);
  }

  /**
   * Create new empty instance for testing.
   */
  @VisibleForTesting
  public static ClusterSpec withNoDefaults() throws ConfigurationException {
    return withNoDefaults(new PropertiesConfiguration());
  }
  @VisibleForTesting
  public static ClusterSpec withNoDefaults(Configuration conf)
  throws ConfigurationException {
    return new ClusterSpec(conf, false);
  }

  private String clusterName;
  private String serviceName;

  private String clusterUser;

  private List<InstanceTemplate> instanceTemplates;
  private int maxStartupRetries;

  private String provider;
  private String endpoint;
  private String identity;
  private String credential;

  private String blobStoreProvider;
  private String blobStoreIdentity;
  private String blobStoreEndpoint;
  private String blobStoreCredential;
  private String blobStoreCacheContainer;

  private String stateStore;
  private String stateStoreContainer;
  private String stateStoreBlob;

  private Float awsEc2SpotPrice;

  private String privateKey;
  private File privateKeyFile;
  private String publicKey;

  private String blobStoreLocationId;

  private TemplateBuilderSpec template;

  private List<String> clientCidrs;
  private Map<String, List<String>> firewallRules;
  
  private String version;
  private String runUrlBase;
  
  private boolean terminateAllOnLaunchFailure;
  private boolean storeClusterInEtcHosts;

  private String awsEc2PlacementGroup;

  private String autoHostnamePrefix;

  private String autoHostnameSuffix;
  
  private String jdkInstallUrl;
  
  private String kerberosRealm;
  
  private Configuration config;
  
  public ClusterSpec() throws ConfigurationException {
    this(new PropertiesConfiguration());
  }

  public ClusterSpec(Configuration config) throws ConfigurationException {
      this(config, true); // load default configs
  }

  /**
   * @throws ConfigurationException if something is wrong
   */
  public ClusterSpec(Configuration userConfig, boolean loadDefaults)
      throws ConfigurationException {

    if (loadDefaults) {
      config = composeWithDefaults(userConfig);
    } else {
      config = ConfigurationUtils.cloneConfiguration(userConfig);
    }

    setClusterName(getString(Property.CLUSTER_NAME));
    setServiceName(getString(Property.SERVICE_NAME));

    setClusterUser(getString(Property.CLUSTER_USER));

    setInstanceTemplates(InstanceTemplate.parse(config));
    setMaxStartupRetries(getInt(Property.MAX_STARTUP_RETRIES, 1));

    setAutoHostnamePrefix(getString(Property.AUTO_HOSTNAME_PREFIX));
    setAutoHostnameSuffix(getString(Property.AUTO_HOSTNAME_SUFFIX));

    setJdkInstallUrl(getString(Property.JDK_INSTALL_URL));
    
    setKerberosRealm(getString(Property.KERBEROS_REALM));
    
    setProvider(getString(Property.PROVIDER));
    setEndpoint(getString(Property.ENDPOINT));
    setIdentity(getString(Property.IDENTITY));
    setCredential(getString(Property.CREDENTIAL));

    setBlobStoreProvider(getString(Property.BLOBSTORE_PROVIDER));
    setBlobStoreEndpoint(getString(Property.BLOBSTORE_ENDPOINT));
    setBlobStoreIdentity(getString(Property.BLOBSTORE_IDENTITY));
    setBlobStoreCredential(getString(Property.BLOBSTORE_CREDENTIAL));
    setBlobStoreCacheContainer(getString(Property.BLOBSTORE_CACHE_CONTAINER));

    setStateStore(getString(Property.STATE_STORE));
    setStateStoreContainer(getString(Property.STATE_STORE_CONTAINER));
    setStateStoreBlob(getString(Property.STATE_STORE_BLOB));

    setAwsEc2SpotPrice(getFloat(Property.AWS_EC2_SPOT_PRICE, (Float) null));

    checkAndSetKeyPair();

    this.template = ConfigToTemplateBuilderSpec.INSTANCE.apply(config);

    setBlobStoreLocationId(getString(Property.BLOBSTORE_LOCATION_ID));
    setClientCidrs(getList(Property.CLIENT_CIDRS));
    
    setTerminateAllOnLaunchFailure(config.getBoolean(
        Property.TERMINATE_ALL_ON_LAUNCH_FAILURE.getConfigName(), Boolean.TRUE));

    setStoreClusterInEtcHosts(config.getBoolean(
        Property.STORE_CLUSTER_IN_ETC_HOSTS.getConfigName(), Boolean.FALSE));

    setAwsEc2PlacementGroup(getString(Property.AWS_EC2_PLACEMENT_GROUP));

    Map<String, List<String>> fr = new HashMap<String, List<String>>();
    String firewallPrefix = Property.FIREWALL_RULES.getConfigName();
    Pattern firewallRuleKeyPattern = Pattern.compile("^".concat(Pattern.quote(firewallPrefix).concat("(?:\\.(.+))?$")));
    for (String key: Iterators.<String>toArray(config.getKeys(), String.class)) {
      Matcher m = firewallRuleKeyPattern.matcher(key);
      if (!m.matches()) continue;

      String role = m.group(1);
      fr.put(role, config.getList(key));
    }
    setFirewallRules(fr);
    
    setVersion(getString(Property.VERSION));
    setRunUrlBase(getString(Property.RUN_URL_BASE));
  }
  
  /**
   * Create a deep object copy. It's not enough to just copy the configuration
   * because the object can also be modified using the setters and the changes
   * are not reflected in the configuration object.
   */
  public ClusterSpec copy() throws ConfigurationException {
    ClusterSpec r = new ClusterSpec(getConfiguration(), true);

    r.setClusterName(getClusterName());
    r.setServiceName(getServiceName());

    r.setClusterUser(getClusterUser());

    r.setInstanceTemplates(Lists.newLinkedList(getInstanceTemplates()));
    r.setMaxStartupRetries(getMaxStartupRetries());

    r.setProvider(getProvider());
    r.setIdentity(getIdentity());
    r.setCredential(getCredential());

    r.setBlobStoreProvider(getBlobStoreProvider());
    r.setBlobStoreIdentity(getBlobStoreIdentity());
    r.setBlobStoreCredential(getBlobStoreCredential());
    r.setBlobStoreCacheContainer(getBlobStoreCacheContainer());

    r.setAwsEc2SpotPrice(getAwsEc2SpotPrice());

    r.setStateStore(getStateStore());
    r.setStateStoreContainer(getStateStoreContainer());
    r.setStateStoreBlob(getStateStoreBlob());

    r.setPrivateKey(getPrivateKey());
    r.setPublicKey(getPublicKey());

    r.setTemplate(getTemplate());

    r.setBlobStoreLocationId(getBlobStoreLocationId());
    r.setClientCidrs(getClientCidrs());

    r.setVersion(getVersion());
    r.setRunUrlBase(getRunUrlBase());
    
    r.setTerminateAllOnLaunchFailure(isTerminateAllOnLaunchFailure());
    r.setStoreClusterInEtcHosts(isStoreClusterInEtcHosts());

    r.setAwsEc2PlacementGroup(getAwsEc2PlacementGroup());

    r.setAutoHostnamePrefix(getAutoHostnamePrefix());
    r.setAutoHostnameSuffix(getAutoHostnameSuffix());

    r.setJdkInstallUrl(getJdkInstallUrl());
    
    r.setKerberosRealm(getKerberosRealm());
    
    return r;
  }

  private String getString(Property key) {
    return config.getString(key.getConfigName(), null);
  }

  private int getInt(Property key, int defaultValue) {
    return config.getInt(key.getConfigName(), defaultValue);
  }

  private Float getFloat(Property key, Float defaultValue) {
    return config.getFloat(key.getConfigName(), defaultValue);
  }
  
  private List<String> getList(Property key) {
    return config.getList(key.getConfigName());
  }
  
  private Configuration composeWithDefaults(Configuration userConfig)
      throws ConfigurationException {
    CompositeConfiguration composed = new CompositeConfiguration();
    composed.addConfiguration(userConfig);
    composed.addConfiguration(new PropertiesConfiguration(
        getClass().getClassLoader().getResource(DEFAULT_PROPERTIES)));
    return composed;
  }

  protected void checkAndSetKeyPair() throws ConfigurationException {
    String pairRepresentation = "";
    try {
      String privateKeyPath = getString(Property.PRIVATE_KEY_FILE);

      String publicKeyPath = getString(Property.PUBLIC_KEY_FILE);
      publicKeyPath = (publicKeyPath == null && privateKeyPath != null) ?
                privateKeyPath + ".pub" : publicKeyPath;
      if(privateKeyPath != null && publicKeyPath != null) {
        pairRepresentation = "(" + privateKeyPath + ", " +
            publicKeyPath + ")";
        KeyPair pair = KeyPair.load(new JSch(), privateKeyPath, publicKeyPath);
        if (pair.isEncrypted()) {
          throw new ConfigurationException("Key pair " + pairRepresentation +
              " is encrypted. Try generating a new passwordless SSH keypair " +
              "(e.g. with ssh-keygen).");
        }
        if (!sameKeyPair(new File(privateKeyPath), new File(publicKeyPath))) {
          throw new ConfigurationException("Both keys should belong " +
              "to the same key pair: " + pairRepresentation);
        }

        setPrivateKey(new File(privateKeyPath));
        setPublicKey(new File(publicKeyPath));
      }
    } catch (JSchException e) {
      throw new ConfigurationException("Invalid key pair: " +
          pairRepresentation, e);

    } catch (IllegalArgumentException e) {
      throw new ConfigurationException("Invalid key: " +
          pairRepresentation, e);

    } catch (IOException e) {
      throw new ConfigurationException("Error reading one of key file: " +
          pairRepresentation, e);
    }
  }

  public List<InstanceTemplate> getInstanceTemplates() {
    return instanceTemplates;
  }

  public InstanceTemplate getInstanceTemplate(final Set<String> roles) {
    for (InstanceTemplate template : instanceTemplates) {
      if (roles.equals(template.getRoles())) {
        return template;
      }
    }
    return null;
  }

  public InstanceTemplate getInstanceTemplate(String... roles) {
    return getInstanceTemplate(Sets.newLinkedHashSet(Lists.newArrayList(roles)));
  }

  public int getMaxStartupRetries() {
    return maxStartupRetries;
  }

  public String getProvider() {
    return provider;
  }

  public boolean isStub() {
    return "stub".equals(getProvider());
  }
  
  /**
   * Optionally specifies the url of the compute provider. For example, for
   * {@code openstack-nova}, it is the keystone url, like:
   * {@code http://localhost:5000/v2.0/}.
   */
  public String getEndpoint() {
    return endpoint;
  }

  public String getIdentity() {
    return identity;
  }

  public String getCredential() {
    return credential;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getBlobStoreProvider() {
    if (blobStoreProvider == null) {
      return getDefaultBlobStoreForComputeProvider();
    }
    return blobStoreProvider;
  }

  /**
   * Probably jclouds should provide a similar mechanism
   */
  private String getDefaultBlobStoreForComputeProvider() {
    Map<String, String> mappings = Maps.newHashMap();

    mappings.put("ec2","aws-s3");
    mappings.put("aws-ec2", "aws-s3");

    mappings.put("cloudservers", "cloudfiles-us");
    mappings.put("cloudservers-us", "cloudfiles-us");
    mappings.put("cloudservers-uk", "cloudfiles-uk");

    if (!mappings.containsKey(provider)) {
      return null;
    }
    return mappings.get(provider);
  }
  
  /**
   * Optionally specifies the url of the blobstore provider. For example, for
   * {@code swift-keystone}, it is the keystone url, like:
   * {@code http://localhost:5000/v2.0/}.
   */
  public String getBlobStoreEndpoint() {
    return blobStoreEndpoint;
  }
  
  public String getBlobStoreIdentity() {
    if (blobStoreIdentity == null) {
      return identity;
    }
    return blobStoreIdentity;
  }

  public String getBlobStoreCredential() {
    if (blobStoreCredential == null) {
      return credential;
    }
    return blobStoreCredential;
  }

  public String getBlobStoreLocationId() {
    return blobStoreLocationId;
  }

  public String getBlobStoreCacheContainer() {
    return blobStoreCacheContainer;
  }

  public String getStateStore() {
    if (stateStore == null) {
      return "local";
    }
    return stateStore;
  }

  public String getStateStoreContainer() {
    return stateStoreContainer;
  }

  public String getStateStoreBlob() {
    if (stateStoreBlob == null && "blob".equals(stateStore)) {
      return "whirr-" + getClusterName();
    }
    return stateStoreBlob;
  }

  @Nullable
  public Float getAwsEc2SpotPrice() {
    return awsEc2SpotPrice;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getPrivateKey() {
    return privateKey;
  }

  public File getPrivateKeyFile() {
     return privateKeyFile;
  }

  public String getPublicKey() {
    return publicKey;
  }

  /**
   * Parameters that define how an instance is built, including hardware, image, and login information.
   * 
   */
  public TemplateBuilderSpec getTemplate() {
    return template;
  }

  public List<String> getClientCidrs() {
    return clientCidrs;
  }

  public Map<String, List<String>> getFirewallRules() {
    return firewallRules;
  }

  public String getVersion() {
    return version;
  }

  public String getRunUrlBase() {
    return runUrlBase;
  }

  public String getClusterUser() {
    return clusterUser;
  }

  public void setInstanceTemplates(List<InstanceTemplate> instanceTemplates) {
    this.instanceTemplates = instanceTemplates;
  }

  public void setMaxStartupRetries(int maxStartupRetries) {
    this.maxStartupRetries = maxStartupRetries;
  }

  public void setProvider(String provider) {
    if ("ec2".equals(provider)) {
      LOG.warn("Please use provider \"aws-ec2\" instead of \"ec2\"");
      provider = "aws-ec2";
    }
    if ("cloudservers".equals(provider)) {
      LOG.warn("Please use provider \"cloudservers-us\" instead of \"cloudservers\"");
      provider = "cloudservers-us";
    }
    if (provider != null) 
        setAutoHostnameForProvider(provider);
    this.provider = provider;
  }

  private void setAutoHostnameForProvider(String provider) {
      if (provider.equals("cloudservers")
          || provider.equals("cloudservers-us")) {
          setAutoHostnamePrefix(null);
          setAutoHostnameSuffix(".static.cloud-ips.com");
      } else if (provider.equals("cloudservers-uk")) {
          setAutoHostnamePrefix(null);
          setAutoHostnameSuffix(".static.cloud-ips.co.uk");
      }
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }    
  
  public void setIdentity(String identity) {
    this.identity = identity;
  }

  public void setCredential(String credential) {
    this.credential = credential;
  }

  public void setBlobStoreProvider(String provider) {
    blobStoreProvider = provider;
  }

  public void setBlobStoreEndpoint(String endpoint) {
    this.blobStoreEndpoint = endpoint;
  }    

  public void setBlobStoreIdentity(String identity) {
    blobStoreIdentity = identity;
  }

  public void setBlobStoreCredential(String credential) {
    blobStoreCredential = credential;
  }

  public void setBlobStoreLocationId(String locationId) {
    blobStoreLocationId = locationId;
  }

  public void setBlobStoreCacheContainer(String container) {
    blobStoreCacheContainer = container;
  }

  public void setStateStore(String type) {
    if (type != null) {
      checkArgument(Sets.newHashSet("local", "blob", "memory", "none").contains(type),
        "Invalid state store. Valid values are local, blob or none.");
    }
    this.stateStore = type;
  }

  public void setStateStoreContainer(String container) {
    checkContainerName(container);
    this.stateStoreContainer = container;
  }

  private void checkContainerName(String name) {
    if (name != null) {
      checkArgument((new DnsNameValidator(3, 63){}).apply(name));
    }
  }

  public void setStateStoreBlob(String blob) {
    this.stateStoreBlob = blob;
  }

  public void setAwsEc2SpotPrice(@Nullable Float value) {
    this.awsEc2SpotPrice = value;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }
  
  public boolean isTerminateAllOnLaunchFailure() {
    return terminateAllOnLaunchFailure;
  }
  public void setTerminateAllOnLaunchFailure(boolean terminateAllOnLaunchFailure) {
    this.terminateAllOnLaunchFailure = terminateAllOnLaunchFailure;
  }

  public boolean isStoreClusterInEtcHosts() {
    return storeClusterInEtcHosts;
  }
  public void setStoreClusterInEtcHosts(boolean storeClusterInEtcHosts) {
    this.storeClusterInEtcHosts = storeClusterInEtcHosts;
  }

  public String getAwsEc2PlacementGroup() {
    return awsEc2PlacementGroup;
  }
    
  public void setAwsEc2PlacementGroup(String awsEc2PlacementGroup) {
    this.awsEc2PlacementGroup = awsEc2PlacementGroup;
  }

  public String getAutoHostnameSuffix() {
    return autoHostnameSuffix;
  }
    
  public void setAutoHostnameSuffix(String autoHostnameSuffix) {
    this.autoHostnameSuffix = autoHostnameSuffix;
  }
  
  public String getAutoHostnamePrefix() {
    return autoHostnamePrefix;
  }
    
  public void setAutoHostnamePrefix(String autoHostnamePrefix) {
    this.autoHostnamePrefix = autoHostnamePrefix;
  }
  
  public String getJdkInstallUrl() {
    return jdkInstallUrl;
  }

  public void setJdkInstallUrl(String jdkInstallUrl) {
    this.jdkInstallUrl = jdkInstallUrl;
  }

  public String getKerberosRealm() {
    return kerberosRealm;
  }

  public void setKerberosRealm(String kerberosRealm) {
    this.kerberosRealm = kerberosRealm;
  }

/**
   * The rsa public key which is authorized to login to your on the cloud nodes.
   * 
   * @param publicKey
   */
  public void setPublicKey(String publicKey) {
    checkPublicKey(publicKey);
    this.publicKey = publicKey;
  }
  
  /**
   * 
   * @throws IOException
   *           if there is a problem reading the file
   * @see #setPublicKey(String)
   */
  public void setPublicKey(File publicKey) throws IOException {
    String key = IOUtils.toString(new FileReader(publicKey));
    checkPublicKey(key);
    this.publicKey = key;
  }

  private void checkPublicKey(String publicKey) {
    /*
     * http://stackoverflow.com/questions/2494645#2494645
     */
    checkArgument(checkNotNull(publicKey, "publicKey").startsWith("ssh-rsa AAAAB3NzaC1yc2EA"),
        "key should start with ssh-rsa AAAAB3NzaC1yc2EA");
  }
  
  /**
   * The rsa private key which is used as the login identity on the cloud 
   * nodes.
   * 
   * @param privateKey
   */
  public void setPrivateKey(String privateKey) {
    checkPrivateKey(privateKey);
    this.privateKey = privateKey;
  }

  /**
   * 
   * @throws IOException
   *           if there is a problem reading the file
   * @see #setPrivateKey(String)
   */
  public void setPrivateKey(File privateKey) throws IOException {
    this.privateKeyFile = privateKey;
    String key = IOUtils.toString(new FileReader(privateKey));
    checkPrivateKey(key);
    this.privateKey = key;
  }
  
  private void checkPrivateKey(String privateKey) {
    checkArgument(checkNotNull(privateKey, "privateKey")
        .startsWith("-----BEGIN RSA PRIVATE KEY-----"),
        "key should start with -----BEGIN RSA PRIVATE KEY-----");
  }
  
  public void setTemplate(TemplateBuilderSpec template) {
     this.template = template;
  }

  public void setClientCidrs(List<String> clientCidrs) {
    this.clientCidrs = clientCidrs;
  }
  
  public void setFirewallRules(Map<String,List<String>> firewallRules) {
    this.firewallRules = firewallRules;
  }
  
  public void setVersion(String version) {
    this.version = version;
  }

  public void setRunUrlBase(String runUrlBase) {
    this.runUrlBase = runUrlBase;
  }

  public void setClusterUser(String user) {
    checkArgument(user == null || !user.equals("root"), "cluster-user != root or do not run as root");
    this.clusterUser = user;
  }

  public Configuration getConfiguration() {
    return config;
  }
  
  public Configuration getConfigurationForKeysWithPrefix(String prefix) {
    Configuration c = new PropertiesConfiguration();
    for (@SuppressWarnings("unchecked")
        Iterator<String> it = config.getKeys(prefix); it.hasNext(); ) {
      String key = it.next();
      c.setProperty(key, config.getProperty(key));
    }
    return c;
  }
  
  public Configuration getConfigurationForKeysMatching(Pattern pattern) {
    Configuration c = new PropertiesConfiguration();
    for (@SuppressWarnings("unchecked")
        Iterator<String> it = config.getKeys(); it.hasNext(); ) {
      String key = it.next();
      if (pattern.matcher(key).matches()) {
        c.setProperty(key, config.getProperty(key));
      }
    }
    return c;
  }
  
  /**
   * @return the directory for storing cluster-related files
   */
  public File getClusterDirectory() {
    File clusterDir = new File(new File(System.getProperty("user.home")), ".whirr");
    clusterDir = new File(clusterDir, getClusterName());
    clusterDir.mkdirs();
    return clusterDir;
  }
    
  public boolean equals(Object o) {
    if (o instanceof ClusterSpec) {
      ClusterSpec that = (ClusterSpec) o;
      return Objects.equal(getInstanceTemplates(), that.getInstanceTemplates())
        && Objects.equal(getMaxStartupRetries(), that.getMaxStartupRetries())
        && Objects.equal(getProvider(), that.getProvider())
        && Objects.equal(getEndpoint(), that.getEndpoint())
        && Objects.equal(getIdentity(), that.getIdentity())
        && Objects.equal(getCredential(), that.getCredential())
        && Objects.equal(getBlobStoreProvider(), that.getBlobStoreProvider())
        && Objects.equal(getBlobStoreEndpoint(), that.getBlobStoreEndpoint())
        && Objects.equal(getBlobStoreIdentity(), that.getBlobStoreIdentity())
        && Objects.equal(getBlobStoreCredential(), that.getBlobStoreCredential())
        && Objects.equal(getBlobStoreCacheContainer(), that.getBlobStoreCacheContainer())
        && Objects.equal(getClusterName(), that.getClusterName())
        && Objects.equal(getServiceName(), that.getServiceName())
        && Objects.equal(getClusterUser(), that.getClusterUser())
        && Objects.equal(getPublicKey(), that.getPublicKey())
        && Objects.equal(getPrivateKey(), that.getPrivateKey())
        && Objects.equal(getTemplate(), that.getTemplate())
        && Objects.equal(getBlobStoreLocationId(), that.getBlobStoreLocationId())
        && Objects.equal(getClientCidrs(), that.getClientCidrs())
        && Objects.equal(getVersion(), that.getVersion())
        && Objects.equal(getRunUrlBase(), that.getRunUrlBase())
        && Objects.equal(getStateStore(), that.getStateStore())
        && Objects.equal(getStateStoreContainer(), that.getStateStoreContainer())
        && Objects.equal(getStateStoreBlob(), that.getStateStoreBlob())
        && Objects.equal(getAwsEc2SpotPrice(), that.getAwsEc2SpotPrice())
        && Objects.equal(getAwsEc2PlacementGroup(), that.getAwsEc2PlacementGroup())
        && Objects.equal(getAutoHostnamePrefix(), that.getAutoHostnamePrefix())
        && Objects.equal(getAutoHostnameSuffix(), that.getAutoHostnameSuffix())
        && Objects.equal(getJdkInstallUrl(), that.getJdkInstallUrl())
        && Objects.equal(getKerberosRealm(), that.getKerberosRealm())
        ;
    }
    return false;
  }
  
  public int hashCode() {
    return Objects.hashCode(
        getInstanceTemplates(),
        getMaxStartupRetries(),
        getProvider(),
        getEndpoint(),
        getIdentity(),
        getCredential(),
        getBlobStoreProvider(),
        getBlobStoreEndpoint(),
        getBlobStoreIdentity(),
        getBlobStoreCredential(),
        getBlobStoreCacheContainer(),
        getClusterName(),
        getServiceName(),
        getClusterUser(),
        getPublicKey(),
        getPrivateKey(),
        getTemplate(),
        getBlobStoreLocationId(),
        getClientCidrs(),
        getVersion(),
        getRunUrlBase(),
        getStateStore(),
        getStateStoreBlob(),
        getStateStoreContainer(),
        getAwsEc2SpotPrice(),
        getAwsEc2PlacementGroup(),
        getAutoHostnamePrefix(),
        getAutoHostnameSuffix(),
        getJdkInstallUrl(),
        getKerberosRealm()
    );
  }
  
  public String toString() {
    return Objects.toStringHelper(this).omitNullValues()
      .add("instanceTemplates", getInstanceTemplates())
      .add("maxStartupRetries", getMaxStartupRetries())
      .add("provider", getProvider())
      .add("endpoint", getEndpoint())
      .add("identity", getIdentity())
      .add("credential", getCredential())
      .add("blobStoreProvider", getBlobStoreProvider())
      .add("blobStoreEndpoint", getBlobStoreEndpoint())
      .add("blobStoreCredential", getBlobStoreCredential())
      .add("blobStoreIdentity", getBlobStoreIdentity())
      .add("blobStoreCacheContainer", getBlobStoreCacheContainer())
      .add("clusterName", getClusterName())
      .add("serviceName", getServiceName())
      .add("clusterUser", getClusterUser())
      .add("publicKey", getPublicKey())
      .add("privateKey", getPrivateKey())
      .add("template", getTemplate())
      .add("blobStoreLocationId", getBlobStoreLocationId())
      .add("clientCidrs", getClientCidrs())
      .add("version", getVersion())
      .add("runUrlBase", getRunUrlBase())
      .add("stateStore", getStateStore())
      .add("stateStoreContainer", getStateStoreContainer())
      .add("stateStoreBlob", getStateStoreBlob())
      .add("awsEc2SpotPrice", getAwsEc2SpotPrice())
      .add("terminateAllOnLauchFailure",isTerminateAllOnLaunchFailure())
      .add("storeClusterInEtcHosts",isStoreClusterInEtcHosts())
      .add("awsEc2PlacementGroup",getAwsEc2PlacementGroup())
      .add("autoHostnamePrefix",getAutoHostnamePrefix())
      .add("autoHostnameSuffix",getAutoHostnameSuffix())
      .add("jdkInstallUrl", getJdkInstallUrl())
      .add("kerberosRealm", getKerberosRealm())
      .toString();
  }
}
