package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.record.YTEdge;
import com.orientechnologies.core.record.YTEntity;
import com.orientechnologies.core.record.YTVertex;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;

/**
 *
 */
public interface YTResultSet extends Spliterator<YTResult>, Iterator<YTResult>, AutoCloseable {

  @Override
  boolean hasNext();

  @Override
  YTResult next();

  default void remove() {
    throw new UnsupportedOperationException();
  }

  void close();

  Optional<OExecutionPlan> getExecutionPlan();

  Map<String, Long> getQueryStats();

  default void reset() {
    throw new UnsupportedOperationException("Implement RESET on " + getClass().getSimpleName());
  }

  default boolean tryAdvance(Consumer<? super YTResult> action) {
    if (hasNext()) {
      action.accept(next());
      return true;
    }
    return false;
  }

  default void forEachRemaining(Consumer<? super YTResult> action) {
    Spliterator.super.forEachRemaining(action);
  }

  default YTResultSet trySplit() {
    return null;
  }

  default long estimateSize() {
    return Long.MAX_VALUE;
  }

  default int characteristics() {
    return ORDERED;
  }

  /**
   * Returns the result set as a stream. IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<YTResult> stream() {
    return StreamSupport.stream(this, false).onClose(this::close);
  }

  default List<YTResult> toList() {
    return stream().toList();
  }

  @Nonnull
  default YTResult findFirst() {
    return stream().findFirst().orElse(null);
  }

  default YTResult findFirstOrThrow() {
    return stream().findFirst().orElseThrow(() -> new IllegalStateException("No result found"));
  }

  default YTEntity findFirstEntity() {
    return entityStream().findFirst().orElse(null);
  }

  default YTEntity findFirstEntityOrThrow() {
    return entityStream().findFirst()
        .orElseThrow(() -> new IllegalStateException("No element found"));
  }

  default YTVertex findFirstVertex() {
    return vertexStream().findFirst().orElse(null);
  }

  default YTVertex findFirstVertexOrThrow() {
    return vertexStream().findFirst()
        .orElseThrow(() -> new IllegalStateException("No vertex found"));
  }

  default YTEdge findFirstEdge() {
    return edgeStream().findFirst().orElse(null);
  }

  default YTEdge findFirstEdgeOrThrow() {
    return edgeStream().findFirst()
        .orElseThrow(() -> new IllegalStateException("No edge found"));
  }

  /**
   * Returns the result set as a stream of elements (filters only the results that are elements -
   * where the isEntity() method returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<YTEntity> entityStream() {
    return StreamSupport.stream(
            new Spliterator<YTEntity>() {
              @Override
              public boolean tryAdvance(Consumer<? super YTEntity> action) {
                while (hasNext()) {
                  YTResult elem = next();
                  if (elem.isEntity()) {
                    action.accept(elem.getEntity().get());
                    return true;
                  }
                }
                return false;
              }

              @Override
              public Spliterator<YTEntity> trySplit() {
                return null;
              }

              @Override
              public long estimateSize() {
                return Long.MAX_VALUE;
              }

              @Override
              public int characteristics() {
                return ORDERED;
              }
            },
            false)
        .onClose(this::close);
  }

  default List<YTEntity> toEntityList() {
    return entityStream().toList();
  }

  /**
   * Returns the result set as a stream of vertices (filters only the results that are vertices -
   * where the isVertex() method returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<YTVertex> vertexStream() {
    return StreamSupport.stream(
            new Spliterator<YTVertex>() {
              @Override
              public boolean tryAdvance(Consumer<? super YTVertex> action) {
                while (hasNext()) {
                  YTResult elem = next();
                  if (elem.isVertex()) {
                    action.accept(elem.getVertex().get());
                    return true;
                  }
                }
                return false;
              }

              @Override
              public Spliterator<YTVertex> trySplit() {
                return null;
              }

              @Override
              public long estimateSize() {
                return Long.MAX_VALUE;
              }

              @Override
              public int characteristics() {
                return ORDERED;
              }
            },
            false)
        .onClose(this::close);
  }

  default List<YTVertex> toVertexList() {
    return vertexStream().toList();
  }

  /**
   * Returns the result set as a stream of vertices (filters only the results that are edges - where
   * the isEdge() method returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<YTEdge> edgeStream() {
    return StreamSupport.stream(
            new Spliterator<YTEdge>() {
              @Override
              public boolean tryAdvance(Consumer<? super YTEdge> action) {
                while (hasNext()) {
                  YTResult nextElem = next();
                  if (nextElem != null && nextElem.isEdge()) {
                    action.accept(nextElem.getEdge().get());
                    return true;
                  }
                }
                return false;
              }

              @Override
              public Spliterator<YTEdge> trySplit() {
                return null;
              }

              @Override
              public long estimateSize() {
                return Long.MAX_VALUE;
              }

              @Override
              public int characteristics() {
                return ORDERED;
              }
            },
            false)
        .onClose(this::close);
  }

  default List<YTEdge> toEdgeList() {
    return edgeStream().toList();
  }
}
