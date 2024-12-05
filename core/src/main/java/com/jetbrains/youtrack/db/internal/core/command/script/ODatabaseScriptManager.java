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
package com.jetbrains.youtrack.db.internal.core.command.script;

import com.jetbrains.youtrack.db.internal.common.concur.resource.OResourcePoolListener;
import com.jetbrains.youtrack.db.internal.common.concur.resource.ResourcePoolFactory;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * Manages Script engines per database. Parsing of function library is done only the first time and
 * when changes.
 *
 * @see CommandScript
 */
public class ODatabaseScriptManager {

  private final OScriptManager scriptManager;
  protected ResourcePoolFactory<String, ScriptEngine> pooledEngines;

  public ODatabaseScriptManager(final OScriptManager iScriptManager, final String iDatabaseName) {
    scriptManager = iScriptManager;

    pooledEngines =
        new ResourcePoolFactory<String, ScriptEngine>(
            new ResourcePoolFactory.ObjectFactoryFactory<String, ScriptEngine>() {
              @Override
              public OResourcePoolListener<String, ScriptEngine> create(final String language) {
                return new OResourcePoolListener<String, ScriptEngine>() {
                  @Override
                  public ScriptEngine createNewResource(String key, Object... args) {
                    final ScriptEngine scriptEngine = scriptManager.getEngine(language);
                    final String library =
                        scriptManager.getLibrary(
                            ODatabaseRecordThreadLocal.instance().get(), language);

                    if (library != null) {
                      try {
                        scriptEngine.eval(library);
                      } catch (ScriptException e) {
                        scriptManager.throwErrorMessage(e, library);
                      }
                    }

                    return scriptEngine;
                  }

                  @Override
                  public boolean reuseResource(
                      String iKey, Object[] iAdditionalArgs, ScriptEngine iValue) {
                    if (language.equals("sql")) {
                      return language.equals(iValue.getFactory().getLanguageName());
                    } else {
                      return !(iValue.getFactory().getLanguageName()).equals("sql");
                    }
                  }
                };
              }
            });
    pooledEngines.setMaxPoolSize(GlobalConfiguration.SCRIPT_POOL.getValueAsInteger());
    pooledEngines.setMaxPartitions(1);
  }

  public ScriptEngine acquireEngine(final String language) {
    return pooledEngines.get(language).getResource(language, 0);
  }

  public void releaseEngine(final String language, ScriptEngine entry) {
    pooledEngines.get(language).returnResource(entry);
  }

  public void close() {
    pooledEngines.close();
  }
}
