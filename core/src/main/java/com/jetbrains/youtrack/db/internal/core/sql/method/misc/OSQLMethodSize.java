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
package com.jetbrains.youtrack.db.internal.core.sql.method.misc;

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;

/**
 *
 */
public class OSQLMethodSize extends OAbstractSQLMethod {

  public static final String NAME = "size";

  public OSQLMethodSize() {
    super(NAME);
  }

  @Override
  public Object execute(
      Object iThis,
      final YTIdentifiable iCurrentRecord,
      final CommandContext iContext,
      final Object ioResult,
      final Object[] iParams) {

    final Number size;
    if (ioResult != null) {
      if (ioResult instanceof YTIdentifiable) {
        size = 1;
      } else {
        size = OMultiValue.getSize(ioResult);
      }
    } else {
      size = 0;
    }

    return size;
  }
}
