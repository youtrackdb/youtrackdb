package com.jetbrains.youtrack.db.internal.core.command.script.transformer;

import com.jetbrains.youtrack.db.internal.core.command.script.ScriptResultSets;
import com.jetbrains.youtrack.db.internal.core.command.script.ScriptResultSet;
import com.jetbrains.youtrack.db.internal.core.command.script.transformer.result.MapTransformer;
import com.jetbrains.youtrack.db.internal.core.command.script.transformer.result.ResultTransformer;
import com.jetbrains.youtrack.db.internal.core.command.script.transformer.resultset.ResultSetTransformer;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.graalvm.polyglot.Value;

/**
 *
 */
public class ScriptTransformerImpl implements ScriptTransformer {

  protected Map<Class, ResultSetTransformer> resultSetTransformers = new HashMap<>();
  protected Map<Class, ResultTransformer> transformers = new LinkedHashMap<>(2);

  public ScriptTransformerImpl() {

    if (!GlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean()) {
      try {
        final Class<?> c = Class.forName("jdk.nashorn.api.scripting.JSObject");
        registerResultTransformer(
            c,
            new ResultTransformer() {
              @Override
              public Result transform(DatabaseSessionInternal db, Object value) {
                ResultInternal internal = new ResultInternal(db);

                final List res = new ArrayList();
                internal.setProperty("value", res);

                for (Object v : ((Map) value).values()) {
                  res.add(new ResultInternal(db, (Identifiable) v));
                }

                return internal;
              }
            });
      } catch (Exception e) {
        // NASHORN NOT INSTALLED, IGNORE IT
      }
    }
    registerResultTransformer(Map.class, new MapTransformer(this));
  }

  @Override
  public ResultSet toResultSet(DatabaseSessionInternal db, Object value) {
    if (value instanceof Value v) {
      if (v.isNull()) {
        return null;
      } else if (v.hasArrayElements()) {
        final List<Object> array = new ArrayList<>((int) v.getArraySize());
        for (int i = 0; i < v.getArraySize(); ++i) {
          array.add(new ResultInternal(db, v.getArrayElement(i).asHostObject()));
        }
        value = array;
      } else if (v.isHostObject()) {
        value = v.asHostObject();
      } else if (v.isString()) {
        value = v.asString();
      } else if (v.isNumber()) {
        value = v.asDouble();
      } else {
        value = v;
      }
    }

    if (value == null) {
      return ScriptResultSets.empty(db);
    }
    if (value instanceof ResultSet) {
      return (ResultSet) value;
    } else if (value instanceof Iterator) {
      return new ScriptResultSet(db, (Iterator) value, this);
    }
    ResultSetTransformer resultSetTransformer = resultSetTransformers.get(value.getClass());

    if (resultSetTransformer != null) {
      return resultSetTransformer.transform(value);
    }
    return defaultResultSet(db, value);
  }

  private ResultSet defaultResultSet(DatabaseSessionInternal db, Object value) {
    return new ScriptResultSet(db, Collections.singletonList(value).iterator(), this);
  }

  @Override
  public Result toResult(DatabaseSessionInternal db, Object value) {

    ResultTransformer transformer = getTransformer(value.getClass());

    if (transformer == null) {
      return defaultTransformer(db, value);
    }
    return transformer.transform(db, value);
  }

  public ResultTransformer getTransformer(final Class clazz) {
    if (clazz != null) {
      for (Map.Entry<Class, ResultTransformer> entry : transformers.entrySet()) {
        if (entry.getKey().isAssignableFrom(clazz)) {
          return entry.getValue();
        }
      }
    }
    return null;
  }

  @Override
  public boolean doesHandleResult(Object value) {
    return getTransformer(value.getClass()) != null;
  }

  private Result defaultTransformer(DatabaseSessionInternal db, Object value) {
    ResultInternal internal = new ResultInternal(db);
    internal.setProperty("value", value);
    return internal;
  }

  public void registerResultTransformer(Class clazz, ResultTransformer transformer) {
    transformers.put(clazz, transformer);
  }

  public void registerResultSetTransformer(Class clazz, ResultSetTransformer transformer) {
    resultSetTransformers.put(clazz, transformer);
  }
}
