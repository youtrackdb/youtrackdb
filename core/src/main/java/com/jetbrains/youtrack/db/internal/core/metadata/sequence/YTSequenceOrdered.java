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

import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

/**
 * @see YTSequenceCached
 * @since 2/28/2015
 * <p>A sequence with sequential guarantees. Even when a transaction is rolled back, there will
 * still be no holes. However, as a result, it is slower.
 */
public class YTSequenceOrdered extends YTSequence {

  public YTSequenceOrdered(final EntityImpl document) {
    super(document);
  }

  public YTSequenceOrdered(YTSequence.CreateParams params, String name) {
    super(params, name);
  }

  @Override
  public long nextWork() throws YTSequenceLimitReachedException {
    return callRetry(
        (db, doc) -> {
          long newValue;
          Long limitValue = getLimitValue(doc);
          var increment = getIncrement(doc);

          if (getOrderType(doc) == SequenceOrderType.ORDER_POSITIVE) {
            newValue = getValue(doc) + increment;
            if (limitValue != null && newValue > limitValue) {
              if (getRecyclable(doc)) {
                newValue = getStart(doc);
              } else {
                throw new YTSequenceLimitReachedException("Limit reached");
              }
            }
          } else {
            newValue = getValue(doc) - increment;
            if (limitValue != null && newValue < limitValue) {
              if (getRecyclable(doc)) {
                newValue = getStart(doc);
              } else {
                throw new YTSequenceLimitReachedException("Limit reached");
              }
            }
          }

          setValue(doc, newValue);
          if (limitValue != null && !getRecyclable(doc)) {
            float tillEnd = (float) Math.abs(limitValue - newValue) / increment;
            float delta = (float) Math.abs(limitValue - getStart(doc)) / increment;
            // warning on 1%
            if (tillEnd <= (delta / 100.f) || tillEnd <= 1) {
              String warningMessage =
                  "Non-recyclable sequence: "
                      + getSequenceName(doc)
                      + " reaching limt, current value: "
                      + newValue
                      + " limit value: "
                      + limitValue
                      + " with step: "
                      + increment;
              OLogManager.instance().warn(this, warningMessage);
            }
          }

          return newValue;
        },
        "next");
  }

  @Override
  protected long currentWork() {
    return callRetry((db, doc) -> getValue(doc), "current");
  }

  @Override
  public long resetWork() {
    return callRetry(
        (db, doc) -> {
          long newValue = getStart(doc);
          setValue(doc, newValue);
          return newValue;
        },
        "reset");
  }

  @Override
  public SEQUENCE_TYPE getSequenceType() {
    return SEQUENCE_TYPE.ORDERED;
  }
}
