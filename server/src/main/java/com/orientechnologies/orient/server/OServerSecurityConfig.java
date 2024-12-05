package com.orientechnologies.orient.server;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.security.OSecurityConfig;
import com.orientechnologies.core.security.OSyslog;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;

public class OServerSecurityConfig implements OSecurityConfig {

  private final OServer server;
  private final OServerConfigurationManager serverCfg;
  private OSyslog sysLog;

  public OServerSecurityConfig(OServer server, OServerConfigurationManager serverCfg) {
    super();
    this.server = server;
    this.serverCfg = serverCfg;
  }

  @Override
  public OSyslog getSyslog() {
    if (sysLog == null && server != null) {
      if (server.getPluginManager() != null) {
        OServerPluginInfo syslogPlugin = server.getPluginManager().getPluginByName("syslog");
        if (syslogPlugin != null) {
          sysLog = (OSyslog) syslogPlugin.getInstance();
        }
      }
    }
    return sysLog;
  }

  @Override
  public String getConfigurationFile() {
    // Default
    String configFile =
        OSystemVariableResolver.resolveSystemVariables("${YOU_TRACK_DB_HOME}/config/security.json");

    String ssf =
        server
            .getContextConfiguration()
            .getValueAsString(YTGlobalConfiguration.SERVER_SECURITY_FILE);
    if (ssf != null) {
      configFile = ssf;
    }
    return configFile;
  }
}
