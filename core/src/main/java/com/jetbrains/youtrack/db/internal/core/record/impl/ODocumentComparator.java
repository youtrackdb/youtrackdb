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
package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.common.util.OPair;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.CommandExecutorSQLSelect;
import java.text.Collator;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Comparator implementation class used by ODocumentSorter class to sort documents following dynamic
 * criteria.
 */
public class ODocumentComparator implements Comparator<YTIdentifiable> {

  private final List<OPair<String, String>> orderCriteria;
  private final CommandContext context;
  private final Collator collator;

  public ODocumentComparator(
      final List<OPair<String, String>> iOrderCriteria, CommandContext iContext) {
    this.orderCriteria = iOrderCriteria;
    this.context = iContext;
    YTDatabaseSessionInternal internal = ODatabaseRecordThreadLocal.instance().get();
    collator =
        Collator.getInstance(
            new Locale(
                internal.get(ATTRIBUTES.LOCALECOUNTRY)
                    + "_"
                    + internal.get(ATTRIBUTES.LOCALELANGUAGE)));
  }

  @SuppressWarnings("unchecked")
  public int compare(final YTIdentifiable iDoc1, final YTIdentifiable iDoc2) {
    if (iDoc1 != null && iDoc1.equals(iDoc2)) {
      return 0;
    }

    Object fieldValue1;
    Object fieldValue2;

    int partialResult = 0;

    for (OPair<String, String> field : orderCriteria) {
      final String fieldName = field.getKey();
      final String ordering = field.getValue();

      fieldValue1 = ((EntityImpl) iDoc1.getRecord()).field(fieldName);
      fieldValue2 = ((EntityImpl) iDoc2.getRecord()).field(fieldName);

      if (fieldValue1 == null && fieldValue2 == null) {
        continue;
      }

      if (fieldValue1 == null) {
        return factor(-1, ordering);
      }

      if (fieldValue2 == null) {
        return factor(1, ordering);
      }

      if (!(fieldValue1 instanceof Comparable<?>)) {
        context.incrementVariable(BasicCommandContext.INVALID_COMPARE_COUNT);
        partialResult = ("" + fieldValue1).compareTo("" + fieldValue2);
      } else {
        try {
          if (collator != null && fieldValue1 instanceof String && fieldValue2 instanceof String) {
            partialResult = collator.compare(fieldValue1, fieldValue2);
          } else {
            partialResult = ((Comparable<Object>) fieldValue1).compareTo(fieldValue2);
          }
        } catch (Exception ignore) {
          context.incrementVariable(BasicCommandContext.INVALID_COMPARE_COUNT);
          partialResult = collator.compare("" + fieldValue1, "" + fieldValue2);
        }
      }
      partialResult = factor(partialResult, ordering);

      if (partialResult != 0) {
        break;
      }

      // CONTINUE WITH THE NEXT FIELD
    }

    return partialResult;
  }

  private int factor(final int partialResult, final String iOrdering) {
    if (iOrdering.equals(CommandExecutorSQLSelect.KEYWORD_DESC))
    // INVERT THE ORDERING
    {
      return partialResult * -1;
    }

    return partialResult;
  }
}
