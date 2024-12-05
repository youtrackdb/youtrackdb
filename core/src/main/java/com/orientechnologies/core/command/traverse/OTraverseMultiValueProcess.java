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
package com.orientechnologies.core.command.traverse;

import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import java.util.Iterator;

public class OTraverseMultiValueProcess extends OTraverseAbstractProcess<Iterator<Object>> {

  private final OTraversePath parentPath;
  protected Object value;
  protected int index = -1;

  public OTraverseMultiValueProcess(
      final OTraverse iCommand, final Iterator<Object> iTarget, OTraversePath parentPath) {
    super(iCommand, iTarget);
    this.parentPath = parentPath;
  }

  public YTIdentifiable process() {
    while (target.hasNext()) {
      value = target.next();
      index++;

      if (value instanceof YTIdentifiable) {

        if (value instanceof YTRID) {
          value = ((YTIdentifiable) value).getRecord();
        }
        final OTraverseAbstractProcess<YTIdentifiable> subProcess =
            new OTraverseRecordProcess(command, (YTIdentifiable) value, getPath());
        command.getContext().push(subProcess);

        return null;
      }
    }

    return pop();
  }

  @Override
  public OTraversePath getPath() {
    return parentPath.appendIndex(index);
  }

  @Override
  public String toString() {
    return "[idx:" + index + "]";
  }
}
