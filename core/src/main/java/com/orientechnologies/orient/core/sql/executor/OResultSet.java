package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
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
public interface OResultSet extends Spliterator<OResult>, Iterator<OResult>, AutoCloseable {

  @Override
  boolean hasNext();

  @Override
  OResult next();

  default void remove() {
    throw new UnsupportedOperationException();
  }

  void close();

  Optional<OExecutionPlan> getExecutionPlan();

  Map<String, Long> getQueryStats();

  default void reset() {
    throw new UnsupportedOperationException("Implement RESET on " + getClass().getSimpleName());
  }

  default boolean tryAdvance(Consumer<? super OResult> action) {
    if (hasNext()) {
      action.accept(next());
      return true;
    }
    return false;
  }

  default void forEachRemaining(Consumer<? super OResult> action) {
    Spliterator.super.forEachRemaining(action);
  }

  default OResultSet trySplit() {
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
  default Stream<OResult> stream() {
    return StreamSupport.stream(this, false).onClose(this::close);
  }

  default List<OResult> toList() {
    return stream().toList();
  }

  @Nonnull
  default OResult findFirst() {
    return stream().findFirst().orElse(null);
  }

  default OResult findFirstOrThrow() {
    return stream().findFirst().orElseThrow(() -> new IllegalStateException("No result found"));
  }

  default YTEntity findFirstElement() {
    return elementStream().findFirst().orElse(null);
  }

  default YTEntity findFirstElementOrThrow() {
    return elementStream().findFirst()
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
   * where the isElement() method returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<YTEntity> elementStream() {
    return StreamSupport.stream(
            new Spliterator<YTEntity>() {
              @Override
              public boolean tryAdvance(Consumer<? super YTEntity> action) {
                while (hasNext()) {
                  OResult elem = next();
                  if (elem.isElement()) {
                    action.accept(elem.getElement().get());
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

  default List<YTEntity> toElementList() {
    return elementStream().toList();
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
                  OResult elem = next();
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
                  OResult nextElem = next();
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
