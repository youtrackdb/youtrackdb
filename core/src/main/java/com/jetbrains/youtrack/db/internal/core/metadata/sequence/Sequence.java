/*
 *
 *  *  Copyright YouTrackDB
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
package com.jetbrains.youtrack.db.internal.core.metadata.sequence;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.thread.NonDaemonThreadFactory;
import com.jetbrains.youtrack.db.internal.common.thread.ThreadPoolExecutorWithLogging;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.exception.SequenceException;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;

/**
 * @since 3/2/2015
 */
public abstract class Sequence {

  private static final ExecutorService sequenceExecutor =
      new ThreadPoolExecutorWithLogging(
          0,
          Runtime.getRuntime().availableProcessors(),
          1,
          TimeUnit.MINUTES,
          new LinkedBlockingQueue<>(1024),
          new NonDaemonThreadFactory("SequenceExecutor"));
  public static final long DEFAULT_START = 0;
  public static final int DEFAULT_INCREMENT = 1;
  public static final int DEFAULT_CACHE = 20;
  public static final Long DEFAULT_LIMIT_VALUE = null;
  public static final boolean DEFAULT_RECYCLABLE_VALUE = false;

  protected static final int DEF_MAX_RETRY =
      GlobalConfiguration.SEQUENCE_MAX_RETRY.getValueAsInteger();
  public static final String CLASS_NAME = "OSequence";

  private static final String FIELD_START = "start";
  private static final String FIELD_INCREMENT = "incr";
  private static final String FIELD_VALUE = "value";
  private static final String FIELD_LIMIT_VALUE = "lvalue";
  private static final String FIELD_ORDER_TYPE = "otype";
  private static final String FIELD_RECYCLABLE = "recycle";
  // initialy set this value to true, so those one who read it can pull upper limit value from
  // document

  private static final String FIELD_NAME = "name";
  private static final String FIELD_TYPE = "type";

  protected RID docRid = new ChangeableRecordId();

  private final ReentrantLock updateLock = new ReentrantLock();

  public static final SequenceOrderType DEFAULT_ORDER_TYPE = SequenceOrderType.ORDER_POSITIVE;

  public static class CreateParams {

    protected Long start = DEFAULT_START;
    protected Integer increment = DEFAULT_INCREMENT;
    // significant only for cached sequences
    protected Integer cacheSize = DEFAULT_CACHE;
    protected Long limitValue = DEFAULT_LIMIT_VALUE;
    protected SequenceOrderType orderType = DEFAULT_ORDER_TYPE;
    protected Boolean recyclable = DEFAULT_RECYCLABLE_VALUE;
    protected Boolean turnLimitOff = false;
    protected Long currentValue = null;

    public CreateParams setStart(Long start) {
      this.start = start;
      return this;
    }

    public CreateParams setIncrement(Integer increment) {
      this.increment = increment;
      return this;
    }

    public CreateParams setCacheSize(Integer cacheSize) {
      this.cacheSize = cacheSize;
      return this;
    }

    public CreateParams setLimitValue(Long limitValue) {
      this.limitValue = limitValue;
      return this;
    }

    public CreateParams setOrderType(SequenceOrderType orderType) {
      this.orderType = orderType;
      return this;
    }

    public CreateParams setRecyclable(boolean recyclable) {
      this.recyclable = recyclable;
      return this;
    }

    public CreateParams setTurnLimitOff(Boolean turnLimitOff) {
      this.turnLimitOff = turnLimitOff;
      return this;
    }

    public CreateParams setCurrentValue(Long currentValue) {
      this.currentValue = currentValue;
      return this;
    }

    public CreateParams() {
    }

    public CreateParams resetNull() {
      start = null;
      increment = null;
      cacheSize = null;
      limitValue = null;
      orderType = null;
      recyclable = null;
      turnLimitOff = false;
      currentValue = null;
      return this;
    }

    public CreateParams setDefaults() {
      this.start = this.start != null ? this.start : DEFAULT_START;
      this.increment = this.increment != null ? this.increment : DEFAULT_INCREMENT;
      this.cacheSize = this.cacheSize != null ? this.cacheSize : DEFAULT_CACHE;
      orderType = orderType == null ? DEFAULT_ORDER_TYPE : orderType;
      recyclable = recyclable == null ? DEFAULT_RECYCLABLE_VALUE : recyclable;
      turnLimitOff = turnLimitOff != null && turnLimitOff;
      return this;
    }

    public Long getStart() {
      return start;
    }

    public Integer getIncrement() {
      return increment;
    }

    public Integer getCacheSize() {
      return cacheSize;
    }

    public Long getLimitValue() {
      return limitValue;
    }

    public SequenceOrderType getOrderType() {
      return orderType;
    }

    public Boolean getRecyclable() {
      return recyclable;
    }

    public Boolean getTurnLimitOff() {
      return turnLimitOff;
    }

    public Long getCurrentValue() {
      return currentValue;
    }
  }

  public enum SEQUENCE_TYPE {
    CACHED((byte) 0),
    ORDERED((byte) 1);

    private final byte val;

    SEQUENCE_TYPE(byte val) {
      this.val = val;
    }

    public byte getVal() {
      return val;
    }

    public static SEQUENCE_TYPE fromVal(byte val) {
      return switch (val) {
        case 0 -> CACHED;
        case 1 -> ORDERED;
        default -> throw new SequenceException("Unknown sequence type: " + val);
      };
    }
  }

  private int maxRetry = DEF_MAX_RETRY;

  protected Sequence(EntityImpl document) {
    Objects.requireNonNull(document);
    docRid = document.getIdentity();
  }

  protected Sequence(CreateParams params, @Nonnull String name) {
    Objects.requireNonNull(name);
    var db = getDatabase();

    docRid =
        db.computeInTx(
            () -> {
              var document = new EntityImpl(CLASS_NAME);

              CreateParams currentParams;
              if (params == null) {
                currentParams = new CreateParams().setDefaults();
              } else {
                currentParams = params;
              }

              initSequence(document, currentParams);
              setName(document, name);
              document.save();

              return document.getIdentity();
            });
  }

  private void initSequence(EntityImpl document, Sequence.CreateParams params) {
    setStart(document, params.start);
    setIncrement(document, params.increment);
    if (params.currentValue == null) {
      setValue(document, params.start);
    } else {
      setValue(document, params.currentValue);
    }
    setLimitValue(document, params.limitValue);
    setOrderType(document, params.orderType);
    setRecyclable(document, params.recyclable);

    setSequenceType(document);
  }

  public boolean updateParams(CreateParams params) throws DatabaseException {
    var db = getDatabase();
    var doc = db.<EntityImpl>load(docRid);
    var result = updateParams(doc, params, false);
    doc.save();
    return result;
  }

  boolean updateParams(EntityImpl document, CreateParams params, boolean executeViaDistributed)
      throws DatabaseException {
    boolean any = false;

    if (params.start != null && this.getStart(document) != params.start) {
      this.setStart(document, params.start);
      any = true;
    }

    if (params.increment != null && this.getIncrement(document) != params.increment) {
      this.setIncrement(document, params.increment);
      any = true;
    }

    if (params.limitValue != null && !params.limitValue.equals(this.getLimitValue(document))) {
      this.setLimitValue(document, params.limitValue);
      any = true;
    }

    if (params.orderType != null && this.getOrderType(document) != params.orderType) {
      this.setOrderType(document, params.orderType);
      any = true;
    }

    if (params.recyclable != null && this.getRecyclable(document) != params.recyclable) {
      this.setRecyclable(document, params.recyclable);
      any = true;
    }

    if (params.turnLimitOff != null && params.turnLimitOff) {
      this.setLimitValue(document, null);
    }

    if (params.currentValue != null && getValue(document) != params.currentValue) {
      this.setValue(document, params.currentValue);
    }

    return any;
  }

  protected static long getValue(EntityImpl doc) {
    if (!doc.hasProperty(FIELD_VALUE)) {
      throw new SequenceException("Value property not found in document");
    }
    return doc.getProperty(FIELD_VALUE);
  }

  protected void setValue(EntityImpl document, long value) {
    document.setProperty(FIELD_VALUE, value);
  }

  protected int getIncrement(EntityImpl document) {
    return document.getProperty(FIELD_INCREMENT);
  }

  protected void setLimitValue(EntityImpl document, Long limitValue) {
    document.setProperty(FIELD_LIMIT_VALUE, limitValue);
  }

  protected Long getLimitValue(EntityImpl document) {
    return document.getProperty(FIELD_LIMIT_VALUE);
  }

  protected void setOrderType(EntityImpl document, SequenceOrderType orderType) {
    document.setProperty(FIELD_ORDER_TYPE, orderType.getValue());
  }

  protected SequenceOrderType getOrderType(EntityImpl document) {
    Byte val = document.getProperty(FIELD_ORDER_TYPE);
    return val == null ? SequenceOrderType.ORDER_POSITIVE : SequenceOrderType.fromValue(val);
  }

  protected void setIncrement(EntityImpl document, int value) {
    document.setProperty(FIELD_INCREMENT, value);
  }

  protected long getStart(EntityImpl document) {
    return document.getProperty(FIELD_START);
  }

  protected void setStart(EntityImpl document, long value) {
    document.setProperty(FIELD_START, value);
  }

  public int getMaxRetry() {
    return maxRetry;
  }

  public void setMaxRetry(final int maxRetry) {
    this.maxRetry = maxRetry;
  }

  public String getName() {
    return getSequenceName(getDatabase().load(docRid));
  }

  protected void setName(EntityImpl doc, final String name) {
    doc.setProperty(FIELD_NAME, name);
  }

  protected boolean getRecyclable(EntityImpl document) {
    return document.getProperty(FIELD_RECYCLABLE);
  }

  protected void setRecyclable(EntityImpl document, final boolean recyclable) {
    document.setProperty(FIELD_RECYCLABLE, recyclable);
  }

  private void setSequenceType(EntityImpl document) {
    document.setProperty(FIELD_TYPE, getSequenceType());
  }

  protected final DatabaseSessionInternal getDatabase() {
    return DatabaseRecordThreadLocal.instance().get();
  }

  public static String getSequenceName(final EntityImpl iDocument) {
    return iDocument.getProperty(FIELD_NAME);
  }

  public static SEQUENCE_TYPE getSequenceType(final EntityImpl document) {
    String sequenceTypeStr = document.field(FIELD_TYPE);
    if (sequenceTypeStr != null) {
      return SEQUENCE_TYPE.valueOf(sequenceTypeStr);
    }

    throw new SequenceException("Sequence type not found in document");
  }

  public static void initClass(DatabaseSession session, SchemaClassImpl sequenceClass) {
    sequenceClass.createProperty(session, Sequence.FIELD_START, PropertyType.LONG,
        (PropertyType) null, true);
    sequenceClass.createProperty(session, Sequence.FIELD_INCREMENT, PropertyType.INTEGER,
        (PropertyType) null,
        true);
    sequenceClass.createProperty(session, Sequence.FIELD_VALUE, PropertyType.LONG,
        (PropertyType) null, true);

    sequenceClass.createProperty(session, Sequence.FIELD_NAME, PropertyType.STRING,
        (PropertyType) null,
        true);
    sequenceClass.createProperty(session, Sequence.FIELD_TYPE, PropertyType.STRING,
        (PropertyType) null,
        true);

    sequenceClass.createProperty(session, Sequence.FIELD_LIMIT_VALUE, PropertyType.LONG,
        (PropertyType) null,
        true);
    sequenceClass.createProperty(session, Sequence.FIELD_ORDER_TYPE, PropertyType.BYTE,
        (PropertyType) null,
        true);
    sequenceClass.createProperty(session, Sequence.FIELD_RECYCLABLE, PropertyType.BOOLEAN,
        (PropertyType) null,
        true);
  }

  /*
   * Forwards the sequence by one, and returns the new value.
   */
  public long next() throws SequenceLimitReachedException, DatabaseException {
    return nextWork();
  }

  public abstract long nextWork() throws SequenceLimitReachedException;

  /*
   * Returns the current sequence value. If next() was never called, returns null
   */
  public long current() throws DatabaseException {
    return currentWork();
  }

  protected abstract long currentWork();

  public long reset() throws DatabaseException {
    return resetWork();
  }

  public abstract long resetWork();

  /*
   * Returns the sequence type
   */
  public abstract SEQUENCE_TYPE getSequenceType();

  protected long callRetry(final SequenceCallable callable, final String method) {

    var oldDb = getDatabase();
    var db = oldDb.copy();
    oldDb.activateOnCurrentThread();

    var future =
        sequenceExecutor.submit(
            () -> {
              db.activateOnCurrentThread();
              try (db) {
                for (int retry = 0; retry < maxRetry; ++retry) {
                  updateLock.lock();
                  try {
                    return db.computeInTx(
                        () -> {
                          var doc = docRid.<EntityImpl>getRecord();
                          var result = callable.call(db, doc);
                          doc.save();
                          return result;
                        });
                  } catch (ConcurrentModificationException ignore) {
                    try {
                      //noinspection BusyWait
                      Thread.sleep(
                          1
                              + new Random()
                              .nextInt(
                                  getDatabase()
                                      .getConfiguration()
                                      .getValueAsInteger(
                                          GlobalConfiguration.SEQUENCE_RETRY_DELAY)));
                    } catch (InterruptedException ignored) {
                      Thread.currentThread().interrupt();
                      break;
                    }

                  } catch (StorageException e) {
                    if (!(e.getCause() instanceof ConcurrentModificationException)) {
                      throw BaseException.wrapException(
                          new SequenceException(
                              "Error in transactional processing of "
                                  + getName()
                                  + "."
                                  + method
                                  + "()"),
                          e);
                    }
                  } catch (Exception e) {
                    db.executeInTx(
                        () -> {
                          throw BaseException.wrapException(
                              new SequenceException(
                                  "Error in transactional processing of "
                                      + getName()
                                      + "."
                                      + method
                                      + "()"),
                              e);
                        });
                  } finally {
                    updateLock.unlock();
                  }
                }
                updateLock.lock();
                try {
                  return db.computeInTx(
                      () -> {
                        var doc = docRid.<EntityImpl>getRecord();
                        var result = callable.call(db, doc);
                        doc.save();
                        return result;
                      });
                } catch (Exception e) {
                  throw BaseException.wrapException(
                      new SequenceException(
                          "Error in transactional processing of "
                              + getName()
                              + "."
                              + method
                              + "()"),
                      e);
                } finally {
                  updateLock.unlock();
                }
              }
            });
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw BaseException.wrapException(
          new DatabaseException("Sequence operation was interrupted"), e);
    } catch (ExecutionException e) {
      var cause = e.getCause();
      if (cause == null) {
        cause = e;
      }
      throw BaseException.wrapException(
          new SequenceException(
              "Error in transactional processing of " + getName() + "." + method + "()"),
          cause);
    }
  }

  @FunctionalInterface
  public interface SequenceCallable {

    long call(DatabaseSession db, EntityImpl doc);
  }
}
