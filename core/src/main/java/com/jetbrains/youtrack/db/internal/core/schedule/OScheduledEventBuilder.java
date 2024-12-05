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

import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.function.OFunction;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.type.ODocumentWrapper;
import java.util.Date;
import java.util.Map;

/**
 * Builds a OSchedulerEvent with a fluent interface
 *
 * @since v2.2
 */
public class OScheduledEventBuilder extends ODocumentWrapper {

  public OScheduledEventBuilder() {
    super(new EntityImpl(OScheduledEvent.CLASS_NAME));
  }

  /**
   * Creates a scheduled event object from a configuration.
   */
  public OScheduledEventBuilder(final EntityImpl doc) {
    super(doc);
  }

  public OScheduledEventBuilder setFunction(YTDatabaseSession session, final OFunction function) {
    getDocument(session).field(OScheduledEvent.PROP_FUNC, function);
    return this;
  }

  public OScheduledEventBuilder setRule(YTDatabaseSession session, final String rule) {
    getDocument(session).field(OScheduledEvent.PROP_RULE, rule);
    return this;
  }

  public OScheduledEventBuilder setArguments(YTDatabaseSession session,
      final Map<Object, Object> arguments) {
    getDocument(session).field(OScheduledEvent.PROP_ARGUMENTS, arguments);
    return this;
  }

  public OScheduledEventBuilder setStartTime(YTDatabaseSession session, final Date startTime) {
    getDocument(session).field(OScheduledEvent.PROP_STARTTIME, startTime);
    return this;
  }

  public OScheduledEvent build(YTDatabaseSessionInternal session) {
    var event = new OScheduledEvent(getDocument(session), session);
    event.save(session);
    return event;
  }

  public String toString() {
    var db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      return getDocument(db).toString();
    }
    return super.toString();
  }

  public OScheduledEventBuilder setName(YTDatabaseSession session, final String name) {
    getDocument(session).field(OScheduledEvent.PROP_NAME, name);
    return this;
  }
}
