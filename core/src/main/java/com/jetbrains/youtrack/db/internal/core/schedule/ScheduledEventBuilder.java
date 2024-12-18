/*
 * Copyright 2010-2012 henryzhao81-at-gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jetbrains.youtrack.db.internal.core.schedule;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.function.Function;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.type.EntityWrapper;
import java.util.Date;
import java.util.Map;

/**
 * Builds a OSchedulerEvent with a fluent interface
 *
 * @since v2.2
 */
public class ScheduledEventBuilder extends EntityWrapper {

  public ScheduledEventBuilder(DatabaseSessionInternal db) {
    super(new EntityImpl(db, ScheduledEvent.CLASS_NAME));
  }

  /**
   * Creates a scheduled event object from a configuration.
   */
  public ScheduledEventBuilder(final EntityImpl entity) {
    super(entity);
  }

  public ScheduledEventBuilder setFunction(DatabaseSession session, final Function function) {
    getDocument(session).field(ScheduledEvent.PROP_FUNC, function);
    return this;
  }

  public ScheduledEventBuilder setRule(DatabaseSession session, final String rule) {
    getDocument(session).field(ScheduledEvent.PROP_RULE, rule);
    return this;
  }

  public ScheduledEventBuilder setArguments(DatabaseSession session,
      final Map<Object, Object> arguments) {
    getDocument(session).field(ScheduledEvent.PROP_ARGUMENTS, arguments);
    return this;
  }

  public ScheduledEventBuilder setStartTime(DatabaseSession session, final Date startTime) {
    getDocument(session).field(ScheduledEvent.PROP_STARTTIME, startTime);
    return this;
  }

  public ScheduledEvent build(DatabaseSessionInternal session) {
    var event = new ScheduledEvent(getDocument(session), session);
    event.save(session);
    return event;
  }

  public String toString() {
    var db = DatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      return getDocument(db).toString();
    }
    return super.toString();
  }

  public ScheduledEventBuilder setName(DatabaseSession session, final String name) {
    getDocument(session).field(ScheduledEvent.PROP_NAME, name);
    return this;
  }
}
