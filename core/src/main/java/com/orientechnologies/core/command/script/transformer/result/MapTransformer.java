package com.orientechnologies.core.command.script.transformer.result;

import com.orientechnologies.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultInternal;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 *
 */
public class MapTransformer implements OResultTransformer<Map<Object, Object>> {

  private final OScriptTransformer transformer;

  public MapTransformer(OScriptTransformer transformer) {
    this.transformer = transformer;
  }

  @Override
  public YTResult transform(YTDatabaseSessionInternal db, Map<Object, Object> element) {
    YTResultInternal internal = new YTResultInternal(db);
    element.forEach(
        (key, val) -> {
          if (transformer.doesHandleResult(val)) {
            internal.setProperty(key.toString(), transformer.toResult(db, val));
          } else {

            if (val instanceof Iterable) {
              Spliterator spliterator = ((Iterable) val).spliterator();
              Object collect =
                  StreamSupport.stream(spliterator, false)
                      .map((e) -> this.transformer.toResult(db, e))
                      .collect(Collectors.toList());
              internal.setProperty(key.toString(), collect);
            } else {
              internal.setProperty(key.toString(), val);
            }
          }
        });
    return internal;
  }
}
