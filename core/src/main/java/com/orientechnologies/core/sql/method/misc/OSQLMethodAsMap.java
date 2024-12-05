/*
 *
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.core.sql.method.misc;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Transforms current value into a Map.
 */
public class OSQLMethodAsMap extends OAbstractSQLMethod {

  public static final String NAME = "asmap";

  public OSQLMethodAsMap() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    if (ioResult instanceof Map)
    // ALREADY A MAP
    {
      return ioResult;
    }

    if (ioResult == null)
    // NULL VALUE, RETURN AN EMPTY MAP
    {
      return Collections.EMPTY_MAP;
    }

    if (ioResult instanceof YTEntityImpl)
    // CONVERT ODOCUMENT TO MAP
    {
      return ((YTEntityImpl) ioResult).toMap();
    }

    Iterator<Object> iter;
    if (ioResult instanceof Iterator<?>) {
      iter = (Iterator<Object>) ioResult;
    } else if (ioResult instanceof Iterable<?>) {
      iter = ((Iterable<Object>) ioResult).iterator();
    } else {
      return null;
    }

    final HashMap<Object, Object> map = new HashMap<Object, Object>();
    while (iter.hasNext()) {
      final Object key = iter.next();
      if (iter.hasNext()) {
        final Object value = iter.next();
        map.put(key, value);
      }
    }

    return map;
  }
}
