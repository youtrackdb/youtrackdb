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
package com.jetbrains.youtrack.db.internal.core.db.tool;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Iterator;
import java.util.List;

/**
 * Repair database tool.
 *
 * @since v2.2.0
 */
public class DatabaseRepair extends DatabaseTool {

  private boolean removeBrokenLinks = true;

  @Override
  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-excludeAll")) {

      removeBrokenLinks = false;

    } else if (option.equalsIgnoreCase("-removeBrokenLinks")) {

      removeBrokenLinks = Boolean.parseBoolean(items.get(0));
    }
  }

  public void run() {
    long errors = 0;

    if (removeBrokenLinks) {
      errors += removeBrokenLinks();
    }

    message("\nRepair database complete (" + errors + " errors)");
  }

  protected long removeBrokenLinks() {
    long fixedLinks = 0L;
    long modifiedEntities = 0L;
    long errors = 0L;

    message("\n- Removing broken links...");
    for (String clusterName : database.getClusterNames()) {
      for (Record rec : database.browseCluster(clusterName)) {
        try {
          if (rec instanceof EntityImpl entity) {
            boolean changed = false;

            for (String fieldName : entity.fieldNames()) {
              final Object fieldValue = entity.rawField(fieldName);

              if (fieldValue instanceof Identifiable) {
                if (fixLink(fieldValue)) {
                  entity.field(fieldName, (Identifiable) null);
                  fixedLinks++;
                  changed = true;
                  if (verbose) {
                    message(
                        "\n--- reset link "
                            + ((Identifiable) fieldValue).getIdentity()
                            + " in field '"
                            + fieldName
                            + "' (rid="
                            + entity.getIdentity()
                            + ")");
                  }
                }
              } else if (fieldValue instanceof Iterable<?>) {
                final Iterator<Object> it = ((Iterable) fieldValue).iterator();
                for (int i = 0; it.hasNext(); ++i) {
                  final Object v = it.next();
                  if (fixLink(v)) {
                    it.remove();
                    fixedLinks++;
                    changed = true;
                    if (verbose) {
                      message(
                          "\n--- reset link "
                              + ((Identifiable) v).getIdentity()
                              + " as item "
                              + i
                              + " in collection of field '"
                              + fieldName
                              + "' (rid="
                              + entity.getIdentity()
                              + ")");
                    }
                  }
                }
              }
            }

            if (changed) {
              modifiedEntities++;
              entity.save();

              if (verbose) {
                message("\n-- updated entity " + entity.getIdentity());
              }
            }
          }
        } catch (Exception ignore) {
          errors++;
        }
      }
    }

    message("\n-- Done! Fixed links: " + fixedLinks + ", modified entities: " + modifiedEntities);
    return errors;
  }

  /**
   * Checks if the link must be fixed.
   *
   * @param fieldValue Field containing the Identifiable (RID or Record)
   * @return true to fix it, otherwise false
   */
  protected boolean fixLink(final Object fieldValue) {
    if (fieldValue instanceof Identifiable) {
      final RID id = ((Identifiable) fieldValue).getIdentity();

      if (id.getClusterId() == 0 && id.getClusterPosition() == 0) {
        return true;
      }

      if (((RecordId) id).isValid()) {
        if (id.isPersistent()) {
          try {
            ((Identifiable) fieldValue).getRecord();
          } catch (RecordNotFoundException rnf) {
            return true;
          }
        } else {
          return true;
        }
      }
    }
    return false;
  }
}
