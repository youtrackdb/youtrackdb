/*
 *
 *  *  Copyright 2014 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import javax.annotation.Nonnull;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/3/2015
 */
public class OSequenceCached extends OSequence {

  private static final String FIELD_CACHE = "cache";
  private long cacheStart;
  private long cacheEnd;
  private volatile boolean firstCache;

  public OSequenceCached(final ODocument document) {
    super(document);

    firstCache = true;
    cacheStart = cacheEnd = getValue(document);
  }

  public OSequenceCached(OSequence.CreateParams params, @Nonnull String name) {
    super(params, name);
    var document = (ODocument) docRid.getRecord();

    if (params == null) {
      params = new CreateParams().setDefaults();
    }

    var db = getDatabase();
    var currentParams = params;
    db.executeInTx(
        () -> {
          var boundDocument = db.bindToSession(document);
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
      ODocument document, OSequence.CreateParams params, boolean executeViaDistributed)
      throws ODatabaseException {
    boolean any = super.updateParams(document, params, executeViaDistributed);
    if (params.cacheSize != null && this.getCacheSize(document) != params.cacheSize) {
      this.setCacheSize(document, params.cacheSize);
      any = true;
    }

    firstCache = true;

    return any;
  }

  private void doRecycle(
      ODocument document,
      long start,
      int cacheSize,
      boolean recyclable,
      SequenceOrderType orderType,
      Long limitValue) {
    if (recyclable) {
      setValue(document, start);
      allocateCache(document, cacheSize, orderType, limitValue);
    } else {
      throw new OSequenceLimitReachedException("Limit reached");
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
  public long next() throws OSequenceLimitReachedException, ODatabaseException {
    checkSecurity();
    return nextWork();
  }

  private void checkSecurity() {
    getDatabase()
        .checkSecurity(
            ORule.ResourceGeneric.CLASS,
            ORole.PERMISSION_UPDATE,
            this.docRid.<ODocument>getRecord().getClassName());
  }

  @Override
  public long nextWork() throws OSequenceLimitReachedException {
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
              OLogManager.instance().warn(this, warningMessage);
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

  private int getCacheSize(ODocument document) {
    return document.getProperty(FIELD_CACHE);
  }

  public final void setCacheSize(ODocument document, int cacheSize) {
    document.setProperty(FIELD_CACHE, cacheSize);
  }

  private void allocateCache(
      ODocument document, int cacheSize, SequenceOrderType orderType, Long limitValue) {
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
