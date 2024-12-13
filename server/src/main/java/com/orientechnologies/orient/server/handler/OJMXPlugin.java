/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */

package com.orientechnologies.orient.server.handler;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBConstants;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.api.exception.ConfigurationException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.ServerPluginAbstract;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public class OJMXPlugin extends ServerPluginAbstract {

  private ObjectName onProfiler;
  private boolean profilerManaged;

  public OJMXPlugin() {
  }

  @Override
  public void config(final OServer oServer, final OServerParameterConfiguration[] iParams) {
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value))
        // DISABLE IT
        {
          return;
        }
      } else if (param.name.equalsIgnoreCase("profilerManaged")) {
        profilerManaged = Boolean.parseBoolean(param.value);
      }
    }

    LogManager.instance()
        .info(this, "JMX plugin installed and active: profilerManaged=%s", profilerManaged);

    final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    try {
      if (profilerManaged) {
        // REGISTER THE PROFILER
        onProfiler = new ObjectName("com.orientechnologies.common.profiler:type=ProfilerMXBean");
        if (mBeanServer.isRegistered(onProfiler)) {
          mBeanServer.unregisterMBean(onProfiler);
        }
        mBeanServer.registerMBean(YouTrackDBEnginesManager.instance().getProfiler(), onProfiler);
      }

    } catch (Exception e) {
      throw BaseException.wrapException(
          new ConfigurationException("Cannot initialize JMX server"), e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.server.handler.ServerHandlerAbstract#shutdown()
   */
  @Override
  public void shutdown() {
    try {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      if (onProfiler != null) {
        if (mBeanServer.isRegistered(onProfiler)) {
          mBeanServer.unregisterMBean(onProfiler);
        }
      }

    } catch (Exception e) {
      LogManager.instance()
          .error(this,
              "YouTrackDB Server v" + YouTrackDBConstants.getVersion() + " unregisterMBean error",
              e);
    }
  }

  @Override
  public String getName() {
    return "jmx";
  }
}
