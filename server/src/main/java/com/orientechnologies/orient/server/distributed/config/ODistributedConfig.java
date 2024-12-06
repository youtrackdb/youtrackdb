package com.orientechnologies.orient.server.distributed.config;

import com.jetbrains.youtrack.db.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilder;
import com.jetbrains.youtrack.db.internal.core.db.config.MulticastConfguration;
import com.jetbrains.youtrack.db.internal.core.db.config.NodeConfigurationBuilder;
import com.jetbrains.youtrack.db.internal.core.exception.ConfigurationException;
import com.orientechnologies.orient.server.config.distributed.OServerDistributedConfiguration;
import com.orientechnologies.orient.server.config.distributed.OServerDistributedNetworkMulticastConfiguration;

public class ODistributedConfig {

  public static OServerDistributedConfiguration fromEnv(OServerDistributedConfiguration distributed)
      throws ConfigurationException {
    final OServerDistributedConfiguration config;
    if (distributed == null) {
      config = new OServerDistributedConfiguration();
      config.enabled = false;
    } else {
      config = distributed;
    }

    validateConfiguration(config);

    return config;
  }

  public static void validateConfiguration(OServerDistributedConfiguration configuration)
      throws ConfigurationException {

    if (configuration.enabled) {

      if (configuration.nodeName == null) {
        throw new ConfigurationException("Node name not specified in the configuration");
      }

      if (configuration.group.name == null) {
        throw new ConfigurationException("Group name not specified in the configuration");
      }
      if (configuration.group.password == null) {
        throw new ConfigurationException("Group password not specified in the configuration");
      }
      if (configuration.quorum == null) {
        throw new ConfigurationException("Quorum not specified in the configuration");
      }

      if (configuration.network.multicast.enabled) {

        if (configuration.network.multicast.ip == null) {
          throw new ConfigurationException(
              "Address not specified in the configuration of multicast");
        }

        if (configuration.network.multicast.port == null) {
          throw new ConfigurationException(
              "Address not specified in the configuration of multicast");
        }

        if (configuration.network.multicast.discoveryPorts == null) {
          throw new ConfigurationException(
              "Address not specified in the configuration of multicast");
        }
      }
    }
  }

  public static YouTrackDBConfig buildConfig(
      ContextConfiguration contextConfiguration, OServerDistributedConfiguration distributed) {

    YouTrackDBConfigBuilder builder = YouTrackDBConfig.builder().fromContext(contextConfiguration);

    NodeConfigurationBuilder nodeConfigurationBuilder = builder.getNodeConfigurationBuilder();

    nodeConfigurationBuilder
        .setNodeName(distributed.nodeName)
        .setQuorum(distributed.quorum)
        .setGroupName(distributed.group.name)
        .setGroupPassword(distributed.group.password);

    OServerDistributedNetworkMulticastConfiguration multicast = distributed.network.multicast;

    nodeConfigurationBuilder.setMulticast(
        MulticastConfguration.builder()
            .setEnabled(multicast.enabled)
            .setIp(multicast.ip)
            .setPort(multicast.port)
            .setDiscoveryPorts(multicast.discoveryPorts)
            .build());

    return builder.build();
  }
}
