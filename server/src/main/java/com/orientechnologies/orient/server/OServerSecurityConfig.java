package com.orientechnologies.orient.server;

import com.jetbrains.youtrack.db.internal.common.parser.OSystemVariableResolver;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.security.OSecurityConfig;
import com.jetbrains.youtrack.db.internal.core.security.OSyslog;
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
            .getValueAsString(GlobalConfiguration.SERVER_SECURITY_FILE);
    if (ssf != null) {
      configFile = ssf;
    }
    return configFile;
  }
}
