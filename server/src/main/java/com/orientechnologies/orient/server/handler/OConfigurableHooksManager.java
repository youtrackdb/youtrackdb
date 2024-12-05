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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.YouTrackDBManager;
import com.orientechnologies.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.hook.YTRecordHook;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHookConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * User: kasper fock Date: 09/11/12 Time: 22:35 Registers hooks defined the in xml configuration.
 *
 * <p>Hooks can be defined in xml as :
 *
 * <p><hooks> <hook class="HookClass"> <parameters> <parameter name="foo" value="bar" />
 * </parameters> </hook> </hooks> In case any parameters is defined the hook class should have a
 * method with following signature: public void config(OServer oServer,
 * OServerParameterConfiguration[] iParams)
 */
public class OConfigurableHooksManager implements ODatabaseLifecycleListener {

  private List<OServerHookConfiguration> configuredHooks;

  public OConfigurableHooksManager(final OServerConfiguration iCfg) {
    configuredHooks = iCfg.hooks;
    if (configuredHooks != null && !configuredHooks.isEmpty()) {
      YouTrackDBManager.instance().addDbLifecycleListener(this);
    }
  }

  public void addHook(OServerHookConfiguration configuration) {
    if (this.configuredHooks == null) {
      configuredHooks = new ArrayList<>();
      YouTrackDBManager.instance().addDbLifecycleListener(this);
    }
    configuredHooks.add(configuration);
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  @Override
  public void onCreate(final YTDatabaseSessionInternal iDatabase) {
    onOpen(iDatabase);
  }

  public void onOpen(YTDatabaseSessionInternal iDatabase) {
    if (!iDatabase.isRemote()) {
      var db = iDatabase;
      for (OServerHookConfiguration hook : configuredHooks) {
        try {
          final YTRecordHook.HOOK_POSITION pos = YTRecordHook.HOOK_POSITION.valueOf(hook.position);
          Class<?> klass = Class.forName(hook.clazz);
          final YTRecordHook h;
          Constructor constructor = null;
          try {
            constructor = klass.getConstructor(YTDatabaseSession.class);
          } catch (NoSuchMethodException ex) {
            // Ignore
          }

          if (constructor != null) {
            h = (YTRecordHook) constructor.newInstance(iDatabase);
          } else {
            h = (YTRecordHook) klass.newInstance();
          }
          if (hook.parameters != null && hook.parameters.length > 0) {
            try {
              final Method m =
                  h.getClass().getDeclaredMethod("config", OServerParameterConfiguration[].class);
              m.invoke(h, new Object[]{hook.parameters});
            } catch (Exception e) {
              OLogManager.instance()
                  .warn(
                      this,
                      "[configure] Failed to configure hook '%s'. Parameters specified but hook don"
                          + " support parameters. Should have a method config with parameters"
                          + " OServerParameterConfiguration[] ",
                      hook.clazz);
            }
          }
          db.registerHook(h, pos);
        } catch (Exception e) {
          OLogManager.instance()
              .error(
                  this,
                  "[configure] Failed to configure hook '%s' due to the an error : ",
                  e,
                  hook.clazz,
                  e.getMessage());
        }
      }
    }
  }

  @Override
  public void onClose(YTDatabaseSessionInternal iDatabase) {
  }

  @Override
  public void onDrop(YTDatabaseSessionInternal iDatabase) {
  }

  @Override
  public void onLocalNodeConfigurationRequest(YTEntityImpl iConfiguration) {
  }

  public String getName() {
    return "HookRegisters";
  }
}
