package com.jetbrains.youtrack.db.internal.core.command.script.transformer;

import com.jetbrains.youtrack.db.internal.core.command.script.OScriptResultSets;
import com.jetbrains.youtrack.db.internal.core.command.script.YTScriptResultSet;
import com.jetbrains.youtrack.db.internal.core.command.script.transformer.result.MapTransformer;
import com.jetbrains.youtrack.db.internal.core.command.script.transformer.result.OResultTransformer;
import com.jetbrains.youtrack.db.internal.core.command.script.transformer.resultset.OResultSetTransformer;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
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
public class OScriptTransformerImpl implements OScriptTransformer {

  protected Map<Class, OResultSetTransformer> resultSetTransformers = new HashMap<>();
  protected Map<Class, OResultTransformer> transformers = new LinkedHashMap<>(2);

  public OScriptTransformerImpl() {

    if (!GlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean()) {
      try {
        final Class<?> c = Class.forName("jdk.nashorn.api.scripting.JSObject");
        registerResultTransformer(
            c,
            new OResultTransformer() {
              @Override
              public YTResult transform(YTDatabaseSessionInternal db, Object value) {
                YTResultInternal internal = new YTResultInternal(db);

                final List res = new ArrayList();
                internal.setProperty("value", res);

                for (Object v : ((Map) value).values()) {
                  res.add(new YTResultInternal(db, (YTIdentifiable) v));
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
  public YTResultSet toResultSet(YTDatabaseSessionInternal db, Object value) {
    if (value instanceof Value v) {
      if (v.isNull()) {
        return null;
      } else if (v.hasArrayElements()) {
        final List<Object> array = new ArrayList<>((int) v.getArraySize());
        for (int i = 0; i < v.getArraySize(); ++i) {
          array.add(new YTResultInternal(db, v.getArrayElement(i).asHostObject()));
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
      return OScriptResultSets.empty(db);
    }
    if (value instanceof YTResultSet) {
      return (YTResultSet) value;
    } else if (value instanceof Iterator) {
      return new YTScriptResultSet(db, (Iterator) value, this);
    }
    OResultSetTransformer oResultSetTransformer = resultSetTransformers.get(value.getClass());

    if (oResultSetTransformer != null) {
      return oResultSetTransformer.transform(value);
    }
    return defaultResultSet(db, value);
  }

  private YTResultSet defaultResultSet(YTDatabaseSessionInternal db, Object value) {
    return new YTScriptResultSet(db, Collections.singletonList(value).iterator(), this);
  }

  @Override
  public YTResult toResult(YTDatabaseSessionInternal db, Object value) {

    OResultTransformer transformer = getTransformer(value.getClass());

    if (transformer == null) {
      return defaultTransformer(db, value);
    }
    return transformer.transform(db, value);
  }

  public OResultTransformer getTransformer(final Class clazz) {
    if (clazz != null) {
      for (Map.Entry<Class, OResultTransformer> entry : transformers.entrySet()) {
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

  private YTResult defaultTransformer(YTDatabaseSessionInternal db, Object value) {
    YTResultInternal internal = new YTResultInternal(db);
    internal.setProperty("value", value);
    return internal;
  }

  public void registerResultTransformer(Class clazz, OResultTransformer transformer) {
    transformers.put(clazz, transformer);
  }

  public void registerResultSetTransformer(Class clazz, OResultSetTransformer transformer) {
    resultSetTransformers.put(clazz, transformer);
  }
}
