package com.orientechnologies.core.sql.executor.resultset;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.sql.executor.OExecutionStep;
import com.orientechnologies.core.sql.executor.YTResult;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface OExecutionStream {

  boolean hasNext(OCommandContext ctx);

  YTResult next(OCommandContext ctx);

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

  static OExecutionStream resultIterator(Iterator<YTResult> iterator) {
    return new OResultIteratorExecutionStream(iterator);
  }

  default OCostMeasureExecutionStream profile(OExecutionStep step) {
    return new OCostMeasureExecutionStream(this, step);
  }

  static OExecutionStream loadIterator(Iterator<? extends YTIdentifiable> iterator) {
    return new OLoaderExecutionStream(iterator);
  }

  static OExecutionStream empty() {
    return OEmptyExecutionStream.EMPTY;
  }

  static OExecutionStream singleton(YTResult result) {
    return new OSingletonExecutionStream(result);
  }

  interface OnClose {

    void close(OCommandContext ctx);
  }

  default OExecutionStream onClose(OnClose onClose) {
    return new OnCloseExecutionStream(this, onClose);
  }

  default Stream<YTResult> stream(OCommandContext ctx) {
    return StreamSupport.stream(
            new Spliterator<YTResult>() {

              @Override
              public boolean tryAdvance(Consumer<? super YTResult> action) {
                if (hasNext(ctx)) {
                  action.accept(next(ctx));
                  return true;
                }
                return false;
              }

              @Override
              public Spliterator<YTResult> trySplit() {
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
