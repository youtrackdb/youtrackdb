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

package com.orientechnologies.orient.core.schedule;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDBInternal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Scheduler default implementation.
 *
 * @since Mar 28, 2013
 */
public class OSchedulerImpl {

  private final ConcurrentHashMap<String, OScheduledEvent> events =
      new ConcurrentHashMap<>();

  private final OxygenDBInternal orientDB;

  public OSchedulerImpl(OxygenDBInternal orientDB) {
    this.orientDB = orientDB;
  }

  public void scheduleEvent(ODatabaseSession session, final OScheduledEvent event) {

    if (event.getDocument(session).getIdentity().isNew()) {
      session.begin();
      event.save(session);
      session.commit();
    }

    if (events.putIfAbsent(event.getName(session), event) == null) {
      String database = session.getName();
      event.schedule(database, "admin", orientDB);
    }
  }

  public OScheduledEvent removeEventInternal(final String eventName) {
    final OScheduledEvent event = events.remove(eventName);

    if (event != null) {
      event.interrupt();
    }
    return event;
  }

  public void removeEvent(ODatabaseSessionInternal session, final String eventName) {
    OLogManager.instance().debug(this, "Removing scheduled event '%s'...", eventName);

    final OScheduledEvent event = removeEventInternal(eventName);

    if (event != null) {
      try {
        ODatabaseSessionInternal.getActiveSession().load(event.getDocument(session).getIdentity());
      } catch (ORecordNotFoundException ignore) {
        // ALREADY DELETED, JUST RETURN
        return;
      }

      // RECORD EXISTS: DELETE THE EVENT RECORD
      session.begin();
      event.getDocument(session).delete();
      session.commit();
    }
  }

  public void updateEvent(ODatabaseSessionInternal session, final OScheduledEvent event) {
    final OScheduledEvent oldEvent = events.remove(event.getName(session));
    if (oldEvent != null) {
      oldEvent.interrupt();
    }
    scheduleEvent(session, event);
    OLogManager.instance()
        .debug(
            this,
            "Updated scheduled event '%s' rid=%s...",
            event,
            event.getDocument(session).getIdentity());
  }

  public Map<String, OScheduledEvent> getEvents() {
    return events;
  }

  public OScheduledEvent getEvent(final String name) {
    return events.get(name);
  }

  public void load(ODatabaseSessionInternal database) {
    if (database.getMetadata().getSchema().existsClass(OScheduledEvent.CLASS_NAME)) {
      final Iterable<ODocument> result = database.browseClass(OScheduledEvent.CLASS_NAME);
      for (ODocument d : result) {
        scheduleEvent(database, new OScheduledEvent(d, database));
      }
    }
  }

  public void close() {
    for (OScheduledEvent event : events.values()) {
      event.interrupt();
    }
    events.clear();
  }

  public static void create(ODatabaseSessionInternal database) {
    if (database
        .getMetadata()
        .getImmutableSchemaSnapshot()
        .existsClass(OScheduledEvent.CLASS_NAME)) {
      return;
    }
    final OClass f = database.getMetadata().getSchema().createClass(OScheduledEvent.CLASS_NAME);
    f.createProperty(database, OScheduledEvent.PROP_NAME, OType.STRING, (OType) null, true)
        .setMandatory(database, true)
        .setNotNull(database, true);
    f.createProperty(database, OScheduledEvent.PROP_RULE, OType.STRING, (OType) null, true)
        .setMandatory(database, true)
        .setNotNull(database, true);
    f.createProperty(database, OScheduledEvent.PROP_ARGUMENTS, OType.EMBEDDEDMAP, (OType) null,
        true);
    f.createProperty(database, OScheduledEvent.PROP_STATUS, OType.STRING, (OType) null, true);
    f.createProperty(database,
            OScheduledEvent.PROP_FUNC,
            OType.LINK,
            database.getMetadata().getSchema().getClass(OFunction.CLASS_NAME), true)
        .setMandatory(database, true)
        .setNotNull(database, true);
    f.createProperty(database, OScheduledEvent.PROP_STARTTIME, OType.DATETIME, (OType) null, true);
  }

  public void initScheduleRecord(ODatabaseSessionInternal session, ODocument doc) {
    String name = doc.field(OScheduledEvent.PROP_NAME);
    final OScheduledEvent event = getEvent(name);
    if (event != null && event.getDocument(session) != doc) {
      throw new ODatabaseException(
          "Scheduled event with name '" + name + "' already exists in database");
    }
    doc.field(OScheduledEvent.PROP_STATUS, OScheduler.STATUS.STOPPED.name());
  }

  public void handleUpdateSchedule(ODatabaseSessionInternal session, ODocument doc) {
    try {
      final String schedulerName = doc.field(OScheduledEvent.PROP_NAME);
      OScheduledEvent event = getEvent(schedulerName);

      if (event != null) {
        // UPDATED EVENT
        final Set<String> dirtyFields = new HashSet<String>(Arrays.asList(doc.getDirtyFields()));

        if (dirtyFields.contains(OScheduledEvent.PROP_NAME)) {
          throw new OValidationException("Scheduled event cannot change name");
        }

        if (dirtyFields.contains(OScheduledEvent.PROP_RULE)) {
          // RULE CHANGED, STOP CURRENT EVENT AND RESCHEDULE IT
          updateEvent(session, new OScheduledEvent(doc, session));
        } else {
          doc.field(OScheduledEvent.PROP_STATUS, OScheduler.STATUS.STOPPED.name());
          event.fromStream(session, doc);
        }
      }

    } catch (Exception ex) {
      OLogManager.instance().error(this, "Error on updating scheduled event", ex);
    }
  }
}
