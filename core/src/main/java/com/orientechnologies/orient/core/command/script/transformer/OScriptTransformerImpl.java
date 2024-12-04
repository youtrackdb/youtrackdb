package com.orientechnologies.orient.core.command.script.transformer;

import com.orientechnologies.orient.core.command.script.OScriptResultSet;
import com.orientechnologies.orient.core.command.script.OScriptResultSets;
import com.orientechnologies.orient.core.command.script.transformer.result.MapTransformer;
import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.command.script.transformer.resultset.OResultSetTransformer;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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

    if (!YTGlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean()) {
      try {
        final Class<?> c = Class.forName("jdk.nashorn.api.scripting.JSObject");
        registerResultTransformer(
            c,
            new OResultTransformer() {
              @Override
              public OResult transform(YTDatabaseSessionInternal db, Object value) {
                OResultInternal internal = new OResultInternal(db);

                final List res = new ArrayList();
                internal.setProperty("value", res);

                for (Object v : ((Map) value).values()) {
                  res.add(new OResultInternal(db, (YTIdentifiable) v));
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
  public OResultSet toResultSet(YTDatabaseSessionInternal db, Object value) {
    if (value instanceof Value v) {
      if (v.isNull()) {
        return null;
      } else if (v.hasArrayElements()) {
        final List<Object> array = new ArrayList<>((int) v.getArraySize());
        for (int i = 0; i < v.getArraySize(); ++i) {
          array.add(new OResultInternal(db, v.getArrayElement(i).asHostObject()));
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
    if (value instanceof OResultSet) {
      return (OResultSet) value;
    } else if (value instanceof Iterator) {
      return new OScriptResultSet(db, (Iterator) value, this);
    }
    OResultSetTransformer oResultSetTransformer = resultSetTransformers.get(value.getClass());

    if (oResultSetTransformer != null) {
      return oResultSetTransformer.transform(value);
    }
    return defaultResultSet(db, value);
  }

  private OResultSet defaultResultSet(YTDatabaseSessionInternal db, Object value) {
    return new OScriptResultSet(db, Collections.singletonList(value).iterator(), this);
  }

  @Override
  public OResult toResult(YTDatabaseSessionInternal db, Object value) {

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

  private OResult defaultTransformer(YTDatabaseSessionInternal db, Object value) {
    OResultInternal internal = new OResultInternal(db);
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
