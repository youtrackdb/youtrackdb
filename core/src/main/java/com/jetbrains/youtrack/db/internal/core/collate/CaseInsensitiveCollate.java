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
package com.jetbrains.youtrack.db.internal.core.collate;

import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.internal.common.comparator.DefaultComparator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Case insensitive collate.
 */
public class CaseInsensitiveCollate extends DefaultComparator implements Collate {

  public static final String NAME = "ci";

  public String getName() {
    return NAME;
  }

  public Object transform(final Object obj) {
    if (obj instanceof String) {
      return ((String) obj).toLowerCase(Locale.ENGLISH);
    }

    if (obj instanceof Set) {
      Set result = new HashSet();
      for (Object o : (Set) obj) {
        result.add(transform(o));
      }
      return result;
    }

    if (obj instanceof List) {
      List result = new ArrayList();
      for (Object o : (List) obj) {
        result.add(transform(o));
      }
      return result;
    }
    return obj;
  }

  @Override
  public int hashCode() {
    return NAME.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }

    final CaseInsensitiveCollate that = (CaseInsensitiveCollate) obj;

    return NAME.equals(NAME);
  }

  @Override
  public int compareForOrderBy(Object objectOne, Object objectTwo) {
    Object newObj1 = transform(objectOne);
    Object newObj2 = transform(objectTwo);
    int result = super.compare(newObj1, newObj2);
    if (result == 0) {
      // case insensitive are the same, fall back to case sensitive to have a decent ordering of
      // upper vs lower case
      result = super.compare(objectOne, objectTwo);
    }

    return result;
  }

  @Override
  public String toString() {
    return "{" + getClass().getSimpleName() + " : name = " + NAME + "}";
  }
}
