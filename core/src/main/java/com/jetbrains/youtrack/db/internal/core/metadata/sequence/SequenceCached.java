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

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import javax.annotation.Nonnull;

/**
 * @since 3/3/2015
 */
public class SequenceCached extends Sequence {

  private static final String FIELD_CACHE = "cache";
  private long cacheStart;
  private long cacheEnd;
  private volatile boolean firstCache;

  public SequenceCached(final EntityImpl document) {
    super(document);

    firstCache = true;
    cacheStart = cacheEnd = getValue(document);
  }

  public SequenceCached(Sequence.CreateParams params, @Nonnull String name) {
    super(params, name);
    var document = (EntityImpl) docRid.getRecord();

    if (params == null) {
      params = new CreateParams().setDefaults();
    }

    var db = getDatabase();
    var currentParams = params;
    db.executeInTx(
        () -> {
          EntityImpl boundDocument;

          if (document.isNotBound(db)) {
            boundDocument = db.bindToSession(document);
          } else {
            boundDocument = document;
          }

          setCacheSize(boundDocument, currentParams.cacheSize);
          cacheStart = cacheEnd = 0L;
          allocateCache(
              boundDocument,
              currentParams.cacheSize,
              getOrderType(boundDocument),
              getLimitValue(boundDocument));
          boundDocument.save();
        });
  }

  @Override
  boolean updateParams(
      EntityImpl document, Sequence.CreateParams params, boolean executeViaDistributed)
      throws DatabaseException {
    boolean any = super.updateParams(document, params, executeViaDistributed);
    if (params.cacheSize != null && this.getCacheSize(document) != params.cacheSize) {
      this.setCacheSize(document, params.cacheSize);
      any = true;
    }

    firstCache = true;

    return any;
  }

  private void doRecycle(
      EntityImpl document,
      long start,
      int cacheSize,
      boolean recyclable,
      SequenceOrderType orderType,
      Long limitValue) {
    if (recyclable) {
      setValue(document, start);
      allocateCache(document, cacheSize, orderType, limitValue);
    } else {
      throw new SequenceLimitReachedException("Limit reached");
    }
  }

  private boolean signalToAllocateCache(
      SequenceOrderType orderType, int increment, Long limitValue) {
    if (orderType == SequenceOrderType.ORDER_POSITIVE) {
      return cacheStart + increment > cacheEnd
          && !(limitValue != null && cacheStart + increment > limitValue);
    } else {
      return cacheStart - increment < cacheEnd
          && !(limitValue != null && cacheStart - increment < limitValue);
    }
  }

  @Override
  public long next() throws SequenceLimitReachedException, DatabaseException {
    checkSecurity();
    return nextWork();
  }

  private void checkSecurity() {
    getDatabase()
        .checkSecurity(
            Rule.ResourceGeneric.CLASS,
            Role.PERMISSION_UPDATE,
            this.docRid.<EntityImpl>getRecord().getClassName());
  }

  @Override
  public long nextWork() throws SequenceLimitReachedException {
    return callRetry(
        (db, doc) -> {
          var orderType = getOrderType(doc);
          var limitValue = getLimitValue(doc);
          var increment = getIncrement(doc);
          var cacheSize = getCacheSize(doc);
          var recyble = getRecyclable(doc);
          var start = getStart(doc);

          if (orderType == SequenceOrderType.ORDER_POSITIVE) {
            if (signalToAllocateCache(orderType, increment, limitValue)) {
              boolean cachedbefore = !firstCache;
              allocateCache(doc, cacheSize, orderType, limitValue);
              if (!cachedbefore) {
                if (limitValue != null && cacheStart + increment > limitValue) {
                  doRecycle(doc, start, cacheSize, recyble, orderType, limitValue);
                } else {
                  cacheStart = cacheStart + increment;
                }
              }
            } else if (limitValue != null && cacheStart + increment > limitValue) {
              doRecycle(doc, start, cacheSize, recyble, orderType, limitValue);
            } else {
              cacheStart = cacheStart + increment;
            }
          } else {
            if (signalToAllocateCache(orderType, increment, limitValue)) {
              boolean cachedbefore = !firstCache;
              allocateCache(doc, cacheSize, orderType, limitValue);
              if (!cachedbefore) {
                if (limitValue != null && cacheStart - increment < limitValue) {
                  doRecycle(doc, start, cacheSize, recyble, orderType, limitValue);
                } else {
                  cacheStart = cacheStart - increment;
                }
              }
            } else if (limitValue != null && cacheStart - increment < limitValue) {
              doRecycle(doc, start, cacheSize, recyble, orderType, limitValue);
            } else {
              cacheStart = cacheStart - increment;
            }
          }

          if (limitValue != null && !recyble) {
            float tillEnd = Math.abs(limitValue - cacheStart) / (float) increment;
            float delta = Math.abs(limitValue - start) / (float) increment;
            // warning on 1%
            if (tillEnd <= (delta / 100.f) || tillEnd <= 1) {
              String warningMessage =
                  "Non-recyclable sequence: "
                      + getSequenceName(doc)
                      + " reaching limt, current value: "
                      + cacheStart
                      + " limit value: "
                      + limitValue
                      + " with step: "
                      + increment;
              LogManager.instance().warn(this, warningMessage);
            }
          }

          firstCache = false;
          return cacheStart;
        },
        "next");
  }

  @Override
  protected long currentWork() {
    return this.cacheStart;
  }

  @Override
  public long resetWork() {
    return callRetry(
        (db, doc) -> {
          long newValue = getStart(doc);
          setValue(doc, newValue);
          firstCache = true;
          allocateCache(doc, getCacheSize(doc), getOrderType(doc), getLimitValue(doc));
          return newValue;
        },
        "reset");
  }

  @Override
  public SEQUENCE_TYPE getSequenceType() {
    return SEQUENCE_TYPE.CACHED;
  }

  private int getCacheSize(EntityImpl document) {
    return document.getProperty(FIELD_CACHE);
  }

  public final void setCacheSize(EntityImpl document, int cacheSize) {
    document.setProperty(FIELD_CACHE, cacheSize);
  }

  private void allocateCache(
      EntityImpl document, int cacheSize, SequenceOrderType orderType, Long limitValue) {
    long value = getValue(document);
    long newValue;
    if (orderType == SequenceOrderType.ORDER_POSITIVE) {
      newValue = value + ((long) getIncrement(document) * cacheSize);
      if (limitValue != null && newValue > limitValue) {
        newValue = limitValue;
      }
    } else {
      newValue = value - ((long) getIncrement(document) * cacheSize);
      if (limitValue != null && newValue < limitValue) {
        newValue = limitValue;
      }
    }
    setValue(document, newValue);
    this.cacheStart = value;
    if (orderType == SequenceOrderType.ORDER_POSITIVE) {
      this.cacheEnd = newValue - 1;
    } else {
      this.cacheEnd = newValue + 1;
    }
    firstCache = false;
  }
}
