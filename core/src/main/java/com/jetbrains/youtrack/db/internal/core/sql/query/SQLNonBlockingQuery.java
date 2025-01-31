/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.sql.query;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.UncaughtExceptionHandler;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestAsynch;
import com.jetbrains.youtrack.db.internal.core.command.CommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;

/**
 * SQL asynchronous query. When executed the caller does not wait for the execution, rather the
 * listener will be called for each item found in the query. SQLAsynchQuery has been built on top of
 * this. NOTE: if you're working with remote databases don't execute any remote call inside the
 * callback function because the network channel is locked until the query command has finished.
 *
 * @see SQLSynchQuery
 */
public class SQLNonBlockingQuery<T extends Object> extends SQLQuery<T>
    implements CommandRequestAsynch {

  private static final long serialVersionUID = 1L;

  public class NonBlockingQueryFuture implements Future, List<Future> {

    protected volatile boolean finished = false;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false; // TODO
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return finished;
    }

    @Override
    public synchronized Object get() throws InterruptedException, ExecutionException {
      while (!finished) {
        wait();
      }
      return SQLNonBlockingQuery.this.getResultListener().getResult();
    }

    @Override
    public synchronized Object get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      while (!finished) {
        wait();
      }
      return SQLNonBlockingQuery.this.getResultListener().getResult();
    }

    @Override
    public int size() {
      return 1;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean contains(Object o) {
      return o == this;
    }

    @Override
    public Iterator<Future> iterator() {
      return new Iterator<Future>() {

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public Future next() {
          return null;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Unsuppored remove on non blocking query result");
        }
      };
    }

    @Override
    public Object[] toArray() {
      return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
      return null;
    }

    @Override
    public boolean add(Future future) {
      return false;
    }

    @Override
    public boolean remove(Object o) {
      return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      return false;
    }

    @Override
    public boolean addAll(Collection<? extends Future> c) {
      return false;
    }

    @Override
    public boolean addAll(int index, Collection<? extends Future> c) {
      return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      return false;
    }

    @Override
    public void clear() {
    }

    @Override
    public Future get(int index) {
      if (index == 0) {
        return this;
      }
      return null;
    }

    public boolean isIdempotent() {
      return false;
    }

    @Override
    public Future set(int index, Future element) {
      return null;
    }

    @Override
    public void add(int index, Future element) {
    }

    @Override
    public Future remove(int index) {
      return get(index);
    }

    @Override
    public int indexOf(Object o) {
      return 0;
    }

    @Override
    public int lastIndexOf(Object o) {
      return 0;
    }

    @Override
    public ListIterator<Future> listIterator() {
      return null;
    }

    @Override
    public ListIterator<Future> listIterator(int index) {
      return null;
    }

    @Override
    public List<Future> subList(int fromIndex, int toIndex) {
      return null;
    }
  }

  /**
   * Empty constructor for unmarshalling.
   */
  public SQLNonBlockingQuery() {
  }

  public SQLNonBlockingQuery(final String iText, final CommandResultListener iResultListener) {
    this(iText, -1, iResultListener);
  }

  public SQLNonBlockingQuery(
      final String iText,
      final int iLimit,
      final String iFetchPlan,
      final Map<Object, Object> iArgs,
      final CommandResultListener iResultListener) {
    this(iText, iLimit, iResultListener);
    this.fetchPlan = iFetchPlan;
    this.parameters = iArgs;
  }

  public SQLNonBlockingQuery(
      final String iText, final int iLimit, final CommandResultListener iResultListener) {
    super(iText);
    limit = iLimit;
    resultListener = iResultListener;
  }

  @Override
  public <RET> RET execute(@Nonnull DatabaseSessionInternal querySession, final Object... iArgs) {
    final var future = new NonBlockingQueryFuture();

    var currentThreadLocal =
        DatabaseRecordThreadLocal.instance().getIfDefined();
    final var db = querySession.copy();
    if (currentThreadLocal != null) {
      currentThreadLocal.activateOnCurrentThread();
    } else {
      DatabaseRecordThreadLocal.instance().set(null);
    }

    var t =
        new Thread(
            () -> {
              db.activateOnCurrentThread();
              try {
                var query =
                    new SQLAsynchQuery<T>(
                        SQLNonBlockingQuery.this.getText(),
                        SQLNonBlockingQuery.this.getResultListener());
                query.setFetchPlan(SQLNonBlockingQuery.this.getFetchPlan());
                query.setLimit(SQLNonBlockingQuery.this.getLimit());
                query.execute(querySession, iArgs);
              } catch (RuntimeException e) {
                if (getResultListener() != null) {
                  getResultListener().end();
                }
                throw e;
              } finally {
                if (db != null) {
                  try {
                    db.close();
                  } catch (Exception e) {
                    LogManager.instance().error(this, "Error during database close", e);
                  }
                }
                try {
                  synchronized (future) {
                    future.finished = true;
                    future.notifyAll();
                  }
                } catch (Exception e) {
                  LogManager.instance().error(this, "", e);
                }
              }
            });

    t.setUncaughtExceptionHandler(new UncaughtExceptionHandler());
    t.start();

    return (RET) future;
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @Override
  public boolean isAsynchronous() {
    return true;
  }
}
