package com.jetbrains.youtrack.db.internal.core.command.script;

import com.jetbrains.youtrack.db.api.exception.CommandScriptException;
import com.jetbrains.youtrack.db.internal.common.concur.resource.ResourcePool;
import com.jetbrains.youtrack.db.internal.common.concur.resource.ResourcePoolListener;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.command.script.transformer.ScriptTransformer;
import com.jetbrains.youtrack.db.internal.core.command.traverse.AbstractScriptExecutor;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.function.Function;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.api.query.ResultSet;
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
public class PolyglotScriptExecutor extends AbstractScriptExecutor
    implements ResourcePoolListener<DatabaseSessionInternal, Context> {

  private final ScriptTransformer transformer;
  protected ConcurrentHashMap<String, ResourcePool<DatabaseSessionInternal, Context>>
      contextPools =
      new ConcurrentHashMap<String, ResourcePool<DatabaseSessionInternal, Context>>();

  public PolyglotScriptExecutor(final String language, ScriptTransformer scriptTransformer) {
    super("javascript".equalsIgnoreCase(language) ? "js" : language);
    this.transformer = scriptTransformer;
  }

  private Context resolveContext(DatabaseSessionInternal database) {
    ResourcePool<DatabaseSessionInternal, Context> pool =
        contextPools.computeIfAbsent(
            database.getName(),
            (k) -> {
              return new ResourcePool<DatabaseSessionInternal, Context>(
                  database.getConfiguration().getValueAsInteger(GlobalConfiguration.SCRIPT_POOL),
                  PolyglotScriptExecutor.this);
            });
    return pool.getResource(database, 0);
  }

  private void returnContext(Context context, DatabaseSessionInternal database) {
    ResourcePool<DatabaseSessionInternal, Context> pool = contextPools.get(database.getName());
    if (pool != null) {
      pool.returnResource(context);
    }
  }

  @Override
  public Context createNewResource(DatabaseSessionInternal database, Object... iAdditionalArgs) {
    final ScriptManager scriptManager =
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

    PolyglotScriptBinding bindings = new PolyglotScriptBinding(ctx.getBindings(language));

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
      DatabaseSessionInternal iKey, Object[] iAdditionalArgs, Context iValue) {
    return true;
  }

  @Override
  public ResultSet execute(DatabaseSessionInternal database, String script, Object... params) {
    preExecute(database, script, params);

    Int2ObjectOpenHashMap<Object> par = new Int2ObjectOpenHashMap<>();

    for (int i = 0; i < params.length; i++) {
      par.put(i, params[i]);
    }
    return execute(database, script, par);
  }

  @Override
  public ResultSet execute(DatabaseSessionInternal database, String script, Map params) {

    preExecute(database, script, params);

    final ScriptManager scriptManager =
        database.getSharedContext().getYouTrackDB().getScriptManager();

    Context ctx = resolveContext(database);
    try {
      PolyglotScriptBinding bindings = new PolyglotScriptBinding(ctx.getBindings(language));

      scriptManager.bindContextVariables(null, bindings, database, null, params);

      Value result = ctx.eval(language, script);
      ResultSet transformedResult = transformer.toResultSet(database, result);
      scriptManager.unbind(null, bindings, null, null);
      return transformedResult;

    } catch (PolyglotException e) {
      final int col = e.getSourceLocation() != null ? e.getSourceLocation().getStartColumn() : 0;
      throw BaseException.wrapException(
          new CommandScriptException("Error on execution of the script", script, col),
          new ScriptException(e));
    } finally {
      returnContext(ctx, database);
    }
  }

  @Override
  public Object executeFunction(
      CommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    DatabaseSessionInternal database = context.getDatabase();
    final Function f = database.getMetadata().getFunctionLibrary().getFunction(functionName);

    database.checkSecurity(Rule.ResourceGeneric.FUNCTION, Role.PERMISSION_READ,
        f.getName(database));

    final ScriptManager scriptManager =
        database.getSharedContext().getYouTrackDB().getScriptManager();

    Context ctx = resolveContext(database);
    try {

      PolyglotScriptBinding bindings = new PolyglotScriptBinding(ctx.getBindings(language));

      scriptManager.bindContextVariables(null, bindings, database, null, iArgs);
      final Object[] args = iArgs == null ? null : iArgs.keySet().toArray();

      Value result = ctx.eval(language, scriptManager.getFunctionInvoke(database, f, args));

      Object finalResult;
      if (result.isNull()) {
        finalResult = null;
      } else if (result.hasArrayElements()) {
        final List<Object> array = new ArrayList<>((int) result.getArraySize());
        for (int i = 0; i < result.getArraySize(); ++i) {
          array.add(new ResultInternal(result.getArrayElement(i).asHostObject()));
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
      throw BaseException.wrapException(
          new CommandScriptException("Error on execution of the script", functionName, col),
          new ScriptException(e));
    } finally {
      returnContext(ctx, database);
    }
  }

  @Override
  public void close(String iDatabaseName) {
    ResourcePool<DatabaseSessionInternal, Context> contextPool =
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
    for (ResourcePool<DatabaseSessionInternal, Context> d : contextPools.values()) {
      for (Context c : d.getAllResources()) {
        c.close();
      }
      d.close();
    }
    contextPools.clear();
  }
}
