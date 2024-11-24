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
package com.orientechnologies.orient.core.metadata.function;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Oxygen;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.ORetryQueryException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Stored function. It contains language and code to execute as a function. The execute() takes
 * parameters. The function is state-less, so can be used by different threads.
 */
public class OFunction extends ODocumentWrapper {

  public static final String CLASS_NAME = "OFunction";
  private OCallable<Object, Map<Object, Object>> callback;

  /**
   * Creates a new function.
   */
  public OFunction(ODatabaseSessionInternal session) {
    super(CLASS_NAME);
    setLanguage(session, "SQL");
  }

  /**
   * Creates a new function wrapping the saved document.
   *
   * @param iDocument Document to assign
   */
  public OFunction(final ODocument iDocument) {
    super(iDocument);
  }

  /**
   * Loads a function.
   *
   * @param iRid RID of the function to load
   */
  public OFunction(final ORecordId iRid) {
    super(iRid.getRecord());
  }

  public String getName(ODatabaseSession session) {
    return getDocument(session).field("name");
  }

  public OFunction setName(ODatabaseSession session, final String iName) {
    getDocument(session).field("name", iName);
    return this;
  }

  public String getCode(ODatabaseSession session) {
    return getDocument(session).field("code");
  }

  public void setCode(ODatabaseSession session, final String iCode) {
    getDocument(session).field("code", iCode);
  }

  public String getLanguage(ODatabaseSession session) {
    return getDocument(session).field("language");
  }

  public void setLanguage(ODatabaseSession session, final String iLanguage) {
    getDocument(session).field("language", iLanguage);
  }

  public List<String> getParameters(ODatabaseSession session) {
    return getDocument(session).field("parameters");
  }

  public OFunction setParameters(ODatabaseSession session, final List<String> iParameters) {
    getDocument(session).field("parameters", iParameters);
    return this;
  }

  public boolean isIdempotent(ODatabaseSession session) {
    final Boolean idempotent = getDocument(session).field("idempotent");
    return idempotent != null && idempotent;
  }

  public OFunction setIdempotent(ODatabaseSession session, final boolean iIdempotent) {
    getDocument(session).field("idempotent", iIdempotent);
    return this;
  }

  public OCallable<Object, Map<Object, Object>> getCallback() {
    return callback;
  }

  public OFunction setCallback(final OCallable<Object, Map<Object, Object>> callback) {
    this.callback = callback;
    return this;
  }

  @Deprecated
  public Object execute(final Object... iArgs) {
    return executeInContext(null, iArgs);
  }

  @Deprecated
  public Object executeInContext(OCommandContext iContext, final Object... iArgs) {
    if (iContext == null) {
      iContext = new OBasicCommandContext();
    }
    var database = iContext.getDatabase();
    final List<String> params = getParameters(database);

    // CONVERT PARAMETERS IN A MAP
    Map<Object, Object> args = null;

    if (iArgs.length > 0) {
      args = new LinkedHashMap<Object, Object>();
      for (int i = 0; i < iArgs.length; ++i) {
        // final Object argValue =
        // ORecordSerializerStringAbstract.getTypeValue(iArgs[i].toString());
        final Object argValue = iArgs[i];

        if (params != null && i < params.size()) {
          args.put(params.get(i), argValue);
        } else {
          args.put("param" + i, argValue);
        }
      }
    }

    if (callback != null) {
      // EXECUTE CALLBACK
      return callback.call(args);
    }

    OScriptExecutor executor =
        database
            .getSharedContext()
            .getOxygenDB()
            .getScriptManager()
            .getCommandManager()
            .getScriptExecutor(getLanguage(database));

    return executor.executeFunction(iContext, getName(database), args);
  }

  public Object executeInContext(@Nonnull OCommandContext iContext,
      @Nonnull final Map<String, Object> iArgs) {
    ODatabaseSessionInternal database = iContext.getDatabase();
    // CONVERT PARAMETERS IN A MAP
    final Map<Object, Object> args = new LinkedHashMap<Object, Object>();

    if (!iArgs.isEmpty()) {
      // PRESERVE THE ORDER FOR PARAMETERS (ARE USED AS POSITIONAL)
      final List<String> params = getParameters(database);
      for (String p : params) {
        args.put(p, iArgs.get(p));
      }
    }

    if (callback != null) {
      // EXECUTE CALLBACK
      return callback.call(args);
    }

    OScriptExecutor executor =
        database
            .getSharedContext()
            .getOxygenDB()
            .getScriptManager()
            .getCommandManager()
            .getScriptExecutor(getLanguage(database));

    return executor.executeFunction(iContext, getName(database), args);
  }

  @Deprecated
  public Object execute(ODatabaseSessionInternal session, final Map<Object, Object> iArgs) {
    final long start = Oxygen.instance().getProfiler().startChrono();

    Object result;
    while (true) {
      try {
        if (callback != null) {
          return callback.call(iArgs);
        }

        OScriptExecutor executor =
            session
                .getSharedContext()
                .getOxygenDB()
                .getScriptManager()
                .getCommandManager()
                .getScriptExecutor(getLanguage(session));

        result = session.computeInTx(() -> executor.execute(session, getCode(session), iArgs));

        break;

      } catch (ONeedRetryException | ORetryQueryException ignore) {
      }
    }

    if (Oxygen.instance().getProfiler().isRecording()) {
      Oxygen.instance()
          .getProfiler()
          .stopChrono(
              "db." + ODatabaseRecordThreadLocal.instance().get().getName() + ".function.execute",
              "Time to execute a function",
              start,
              "db.*.function.execute");
    }

    return result;
  }

  public ORID getId(ODatabaseSession session) {
    return getDocument(session).getIdentity();
  }

  @Override
  public String toString() {
    var database = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (database != null) {
      return getName(database);
    }

    return super.toString();
  }
}
