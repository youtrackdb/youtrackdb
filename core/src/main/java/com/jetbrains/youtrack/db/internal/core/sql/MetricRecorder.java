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

package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class MetricRecorder {

  protected CommandContext context;

  public void setContext(CommandContext context) {
    this.context = context;
  }

  public void recordOrderByOptimizationMetric(
      boolean indexIsUsedInOrderBy, boolean fullySortedByIndex) {
    if (context.isRecordingMetrics()) {
      context.setVariable("indexIsUsedInOrderBy", indexIsUsedInOrderBy);
      context.setVariable("fullySortedByIndex", fullySortedByIndex);
    }
  }

  public void recordInvolvedIndexesMetric(Index index) {
    if (context.isRecordingMetrics()) {
      Set<String> idxNames = (Set<String>) context.getVariable("involvedIndexes");
      if (idxNames == null) {
        idxNames = new HashSet<String>();
        context.setVariable("involvedIndexes", idxNames);
      }
      if (index instanceof ChainedIndexProxy) {
        idxNames.addAll(((ChainedIndexProxy) index).getIndexNames());
      } else {
        idxNames.add(index.getName());
      }
    }
  }

  CommandContext orderByElapsed(long startOrderBy) {
    return context.setVariable("orderByElapsed", (System.currentTimeMillis() - startOrderBy));
  }

  public void recordRangeQueryConvertedInBetween() {
    if (context.isRecordingMetrics()) {
      Integer counter = (Integer) context.getVariable("rangeQueryConvertedInBetween");
      if (counter == null) {
        counter = 0;
      }

      counter++;
      context.setVariable("rangeQueryConvertedInBetween", counter);
    }
  }
}
