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
package com.orientechnologies.core.command.script;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.command.OCommandExecutorAbstract;
import com.orientechnologies.core.command.OCommandRequest;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.metadata.function.OFunction;
import com.orientechnologies.core.metadata.security.ORole;
import com.orientechnologies.core.metadata.security.ORule;
import java.util.Map;
import java.util.Map.Entry;
import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * Executes Script Commands.
 *
 * @see OCommandScript
 */
public class OCommandExecutorFunction extends OCommandExecutorAbstract {

  protected OCommandFunction request;

  public OCommandExecutorFunction() {
  }

  @SuppressWarnings("unchecked")
  public OCommandExecutorFunction parse(final OCommandRequest iRequest) {
    request = (OCommandFunction) iRequest;
    return this;
  }

  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    return executeInContext(null, iArgs);
  }

  public Object executeInContext(final OCommandContext iContext, final Map<Object, Object> iArgs) {

    parserText = request.getText();

    YTDatabaseSessionInternal db = iContext.getDatabase();
    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(parserText);

    db.checkSecurity(ORule.ResourceGeneric.FUNCTION, ORole.PERMISSION_READ, f.getName(db));

    final OScriptManager scriptManager = db.getSharedContext().getYouTrackDB().getScriptManager();

    final ScriptEngine scriptEngine =
        scriptManager.acquireDatabaseEngine(db.getName(), f.getLanguage(db));
    try {
      final Bindings binding =
          scriptManager.bindContextVariables(
              scriptEngine,
              scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE),
              db,
              iContext,
              iArgs);

      try {
        final Object result;

        if (scriptEngine instanceof Invocable invocableEngine) {
          // INVOKE AS FUNCTION. PARAMS ARE PASSED BY POSITION
          Object[] args = null;
          if (iArgs != null) {
            args = new Object[iArgs.size()];
            int i = 0;
            for (Entry<Object, Object> arg : iArgs.entrySet()) {
              args[i++] = arg.getValue();
            }
          } else {
            args = OCommonConst.EMPTY_OBJECT_ARRAY;
          }
          result = invocableEngine.invokeFunction(parserText, args);

        } else {
          // INVOKE THE CODE SNIPPET
          final Object[] args = iArgs == null ? null : iArgs.values().toArray();
          result = scriptEngine.eval(scriptManager.getFunctionInvoke(db, f, args), binding);
        }
        return OCommandExecutorUtility.transformResult(
            scriptManager.handleResult(f.getLanguage(db), result, scriptEngine, binding, db));

      } catch (ScriptException e) {
        throw YTException.wrapException(
            new YTCommandScriptException(
                "Error on execution of the script", request.getText(), e.getColumnNumber()),
            e);
      } catch (NoSuchMethodException e) {
        throw YTException.wrapException(
            new YTCommandScriptException("Error on execution of the script", request.getText(), 0),
            e);
      } catch (YTCommandScriptException e) {
        // PASS THROUGH
        throw e;

      } finally {
        scriptManager.unbind(scriptEngine, binding, iContext, iArgs);
      }
    } finally {
      scriptManager.releaseDatabaseEngine(f.getLanguage(db), db.getName(), scriptEngine);
    }
  }

  public boolean isIdempotent() {
    return false;
  }

  @Override
  protected void throwSyntaxErrorException(String iText) {
    throw new YTCommandScriptException(
        "Error on execution of the script: " + iText, request.getText(), 0);
  }
}
