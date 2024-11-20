package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OExecutionStep;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface OExecutionStream {

  boolean hasNext(OCommandContext ctx);

  OResult next(OCommandContext ctx);

  void close(OCommandContext ctx);

  default OExecutionStream map(OResultMapper mapper) {
    return new OMapperExecutionStream(this, mapper);
  }

  default OExecutionStream filter(OFilterResult filter) {
    return new OFilterExecutionStream(this, filter);
  }

  default OExecutionStream flatMap(OMapExecutionStream map) {
    return new OFlatMapExecutionStream(this, map);
  }

  default OExecutionStream interruptable() {
    return new OInterruptResultSet(this);
  }

  default OExecutionStream limit(long limit) {
    return new OLimitedExecutionStream(this, limit);
  }

  static OExecutionStream iterator(Iterator<?> iterator) {
    return new OIteratorExecutionStream(iterator);
  }

  static OExecutionStream resultIterator(Iterator<OResult> iterator) {
    return new OResultIteratorExecutionStream(iterator);
  }

  default OCostMeasureExecutionStream profile(OExecutionStep step) {
    return new OCostMeasureExecutionStream(this, step);
  }

  static OExecutionStream loadIterator(Iterator<? extends OIdentifiable> iterator) {
    return new OLoaderExecutionStream(iterator);
  }

  static OExecutionStream empty() {
    return OEmptyExecutionStream.EMPTY;
  }

  static OExecutionStream singleton(OResult result) {
    return new OSingletonExecutionStream(result);
  }

  interface OnClose {

    void close(OCommandContext ctx);
  }

  default OExecutionStream onClose(OnClose onClose) {
    return new OnCloseExecutionStream(this, onClose);
  }

  default Stream<OResult> stream(OCommandContext ctx) {
    return StreamSupport.stream(
            new Spliterator<OResult>() {

              @Override
              public boolean tryAdvance(Consumer<? super OResult> action) {
                if (hasNext(ctx)) {
                  action.accept(next(ctx));
                  return true;
                }
                return false;
              }

              @Override
              public Spliterator<OResult> trySplit() {
                return null;
              }

              @Override
              public long estimateSize() {
                return Long.MAX_VALUE;
              }

              @Override
              public int characteristics() {
                return 0;
              }
            },
            false)
        .onClose(() -> this.close(ctx));
  }
}
