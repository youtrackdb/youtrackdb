package com.jetbrains.youtrack.db.internal.server.handler;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.ConfigurationException;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.parser.SystemVariableResolver;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.functions.CustomSQLFunctionFactory;
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import com.jetbrains.youtrack.db.internal.server.config.ServerParameterConfiguration;
import com.jetbrains.youtrack.db.internal.server.plugin.ServerPluginAbstract;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Server Plugin to register custom SQL functions.
 */
public class CustomSQLFunctionPlugin extends ServerPluginAbstract {

  private static final char PREFIX_NAME_SEPARATOR = '_';

  private EntityImpl configuration;

  @Override
  public String getName() {
    return "custom-sql-functions-manager";
  }

  @Override
  public void config(YouTrackDBServer youTrackDBServer, ServerParameterConfiguration[] iParams) {
    configuration = new EntityImpl(null);

    final File configFile =
        Arrays.stream(iParams)
            .filter(p -> p.name.equalsIgnoreCase("config"))
            .map(p -> p.value.trim())
            .map(SystemVariableResolver::resolveSystemVariables)
            .map(File::new)
            .filter(File::exists)
            .findFirst()
            .orElseThrow(
                () ->
                    new ConfigurationException(
                        "Custom SQL functions configuration file not found"));

    try {
      String configurationContent = IOUtils.readFileAsString(configFile);
      configurationContent = removeComments(configurationContent);
      configuration = new EntityImpl(null);
      configuration.updateFromJSON(configurationContent);
    } catch (IOException e) {
      throw BaseException.wrapException(
          new ConfigurationException(
              "Cannot load Custom SQL configuration file '"
                  + configFile
                  + "'. No custom functions will be disabled"),
          e);
    }
  }

  private String removeComments(String configurationContent) {
    if (configurationContent == null) {
      return null;
    }
    StringBuilder result = new StringBuilder();
    String[] split = configurationContent.split("\n");
    boolean first = true;
    for (int i = 0; i < split.length; i++) {
      String row = split[i];
      if (row.trim().startsWith("//")) {
        continue;
      }
      if (!first) {
        result.append("\n");
      }
      result.append(row);
      first = false;
    }
    return result.toString();
  }

  @Override
  public void startup() {
    if (Boolean.TRUE.equals(configuration.field("enabled"))) {
      List<Map<String, String>> functions = configuration.field("functions");
      for (Map<String, String> function : functions) {
        final String prefix = function.get("prefix");
        final String clazz = function.get("class");
        if (prefix == null || clazz == null) {
          throw new ConfigurationException(
              "Unable to load functions without prefix and / or class ");
        }
        if (!prefix.matches("^[\\pL]+$")) {
          throw new ConfigurationException(
              "Unable to load functions with prefix '"
                  + prefix
                  + "'. Prefixes can be letters only");
        }

        try {
          Class functionsClass = Class.forName(clazz);
          CustomSQLFunctionFactory.register(prefix + PREFIX_NAME_SEPARATOR, functionsClass);
        } catch (ClassNotFoundException e) {
          throw BaseException.wrapException(
              new ConfigurationException(
                  "Unable to load class " + clazz + " for custom functions with prefix " + prefix),
              e);
        }
      }
    }
  }
}
