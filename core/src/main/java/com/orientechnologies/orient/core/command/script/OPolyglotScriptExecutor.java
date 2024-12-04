package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.traverse.OAbstractScriptExecutor;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.script.ScriptException;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;

/**
 *
 */
public class OPolyglotScriptExecutor extends OAbstractScriptExecutor
    implements OResourcePoolListener<YTDatabaseSessionInternal, Context> {

  private final OScriptTransformer transformer;
  protected ConcurrentHashMap<String, OResourcePool<YTDatabaseSessionInternal, Context>>
      contextPools =
      new ConcurrentHashMap<String, OResourcePool<YTDatabaseSessionInternal, Context>>();

  public OPolyglotScriptExecutor(final String language, OScriptTransformer scriptTransformer) {
    super("javascript".equalsIgnoreCase(language) ? "js" : language);
    this.transformer = scriptTransformer;
  }

  private Context resolveContext(YTDatabaseSessionInternal database) {
    OResourcePool<YTDatabaseSessionInternal, Context> pool =
        contextPools.computeIfAbsent(
            database.getName(),
            (k) -> {
              return new OResourcePool<YTDatabaseSessionInternal, Context>(
                  database.getConfiguration().getValueAsInteger(YTGlobalConfiguration.SCRIPT_POOL),
                  OPolyglotScriptExecutor.this);
            });
    return pool.getResource(database, 0);
  }

  private void returnContext(Context context, YTDatabaseSessionInternal database) {
    OResourcePool<YTDatabaseSessionInternal, Context> pool = contextPools.get(database.getName());
    if (pool != null) {
      pool.returnResource(context);
    }
  }

  @Override
  public Context createNewResource(YTDatabaseSessionInternal database, Object... iAdditionalArgs) {
    final OScriptManager scriptManager =
        database.getSharedContext().getYouTrackDB().getScriptManager();

    final Set<String> allowedPackaged = scriptManager.getAllowedPackages();

    Context ctx =
        Context.newBuilder()
            .allowHostAccess(HostAccess.ALL)
            .allowNativeAccess(false)
            .allowCreateProcess(false)
            .allowCreateThread(false)
            .allowIO(false)
            .allowHostClassLoading(false)
            .allowHostClassLookup(
                s -> {
                  if (allowedPackaged.contains(s)) {
                    return true;
                  }

                  final int pos = s.lastIndexOf('.');
                  if (pos > -1) {
                    return allowedPackaged.contains(s.substring(0, pos) + ".*");
                  }
                  return false;
                })
            .build();

    OPolyglotScriptBinding bindings = new OPolyglotScriptBinding(ctx.getBindings(language));

    scriptManager.bindContextVariables(null, bindings, database, null, null);
    final String library =
        scriptManager.getLibrary(
            database, "js".equalsIgnoreCase(language) ? "javascript" : language);
    if (library != null) {
      ctx.eval(language, library);
    }
    scriptManager.unbind(null, bindings, null, null);
    return ctx;
  }

  @Override
  public boolean reuseResource(
      YTDatabaseSessionInternal iKey, Object[] iAdditionalArgs, Context iValue) {
    return true;
  }

  @Override
  public OResultSet execute(YTDatabaseSessionInternal database, String script, Object... params) {
    preExecute(database, script, params);

    Int2ObjectOpenHashMap<Object> par = new Int2ObjectOpenHashMap<>();

    for (int i = 0; i < params.length; i++) {
      par.put(i, params[i]);
    }
    return execute(database, script, par);
  }

  @Override
  public OResultSet execute(YTDatabaseSessionInternal database, String script, Map params) {

    preExecute(database, script, params);

    final OScriptManager scriptManager =
        database.getSharedContext().getYouTrackDB().getScriptManager();

    Context ctx = resolveContext(database);
    try {
      OPolyglotScriptBinding bindings = new OPolyglotScriptBinding(ctx.getBindings(language));

      scriptManager.bindContextVariables(null, bindings, database, null, params);

      Value result = ctx.eval(language, script);
      OResultSet transformedResult = transformer.toResultSet(database, result);
      scriptManager.unbind(null, bindings, null, null);
      return transformedResult;

    } catch (PolyglotException e) {
      final int col = e.getSourceLocation() != null ? e.getSourceLocation().getStartColumn() : 0;
      throw OException.wrapException(
          new OCommandScriptException("Error on execution of the script", script, col),
          new ScriptException(e));
    } finally {
      returnContext(ctx, database);
    }
  }

  @Override
  public Object executeFunction(
      OCommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    YTDatabaseSessionInternal database = context.getDatabase();
    final OFunction f = database.getMetadata().getFunctionLibrary().getFunction(functionName);

    database.checkSecurity(ORule.ResourceGeneric.FUNCTION, ORole.PERMISSION_READ,
        f.getName(database));

    final OScriptManager scriptManager =
        database.getSharedContext().getYouTrackDB().getScriptManager();

    Context ctx = resolveContext(database);
    try {

      OPolyglotScriptBinding bindings = new OPolyglotScriptBinding(ctx.getBindings(language));

      scriptManager.bindContextVariables(null, bindings, database, null, iArgs);
      final Object[] args = iArgs == null ? null : iArgs.keySet().toArray();

      Value result = ctx.eval(language, scriptManager.getFunctionInvoke(database, f, args));

      Object finalResult;
      if (result.isNull()) {
        finalResult = null;
      } else if (result.hasArrayElements()) {
        final List<Object> array = new ArrayList<>((int) result.getArraySize());
        for (int i = 0; i < result.getArraySize(); ++i) {
          array.add(new OResultInternal(result.getArrayElement(i).asHostObject()));
        }
        finalResult = array;
      } else if (result.isHostObject()) {
        finalResult = result.asHostObject();
      } else if (result.isString()) {
        finalResult = result.asString();
      } else if (result.isNumber()) {
        finalResult = result.asDouble();
      } else {
        finalResult = result;
      }
      scriptManager.unbind(null, bindings, null, null);
      return finalResult;
    } catch (PolyglotException e) {
      final int col = e.getSourceLocation() != null ? e.getSourceLocation().getStartColumn() : 0;
      throw OException.wrapException(
          new OCommandScriptException("Error on execution of the script", functionName, col),
          new ScriptException(e));
    } finally {
      returnContext(ctx, database);
    }
  }

  @Override
  public void close(String iDatabaseName) {
    OResourcePool<YTDatabaseSessionInternal, Context> contextPool =
        contextPools.remove(iDatabaseName);
    if (contextPool != null) {
      for (Context c : contextPool.getAllResources()) {
        c.close();
      }
      contextPool.close();
    }
  }

  @Override
  public void closeAll() {
    for (OResourcePool<YTDatabaseSessionInternal, Context> d : contextPools.values()) {
      for (Context c : d.getAllResources()) {
        c.close();
      }
      d.close();
    }
    contextPools.clear();
  }
}
