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

package com.orientechnologies.core.schedule;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.YouTrackDBInternal;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.exception.YTValidationException;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.function.OFunction;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
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

  private static final String RIDS_OF_EVENTS_TO_RESCHEDULE_KEY =
      OSchedulerImpl.class.getName() + ".ridsOfEventsToReschedule";

  private final ConcurrentHashMap<String, OScheduledEvent> events =
      new ConcurrentHashMap<>();

  private final YouTrackDBInternal youtrackDB;

  public OSchedulerImpl(YouTrackDBInternal youtrackDB) {
    this.youtrackDB = youtrackDB;
  }

  public void scheduleEvent(YTDatabaseSession session, final OScheduledEvent event) {
    if (events.putIfAbsent(event.getName(session), event) == null) {
      String database = session.getName();
      event.schedule(database, "admin", youtrackDB);
    }
  }

  public OScheduledEvent removeEventInternal(final String eventName) {
    final OScheduledEvent event = events.remove(eventName);

    if (event != null) {
      event.interrupt();
    }
    return event;
  }

  public void removeEvent(YTDatabaseSessionInternal session, final String eventName) {
    OLogManager.instance().debug(this, "Removing scheduled event '%s'...", eventName);

    final OScheduledEvent event = removeEventInternal(eventName);

    if (event != null) {
      try {
        session.load(event.getDocument(session).getIdentity());
      } catch (YTRecordNotFoundException ignore) {
        // ALREADY DELETED, JUST RETURN
        return;
      }

      // RECORD EXISTS: DELETE THE EVENT RECORD
      session.begin();
      event.getDocument(session).delete();
      session.commit();
    }
  }

  public void updateEvent(YTDatabaseSessionInternal session, final OScheduledEvent event) {
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

  public void load(YTDatabaseSessionInternal database) {
    if (database.getMetadata().getSchema().existsClass(OScheduledEvent.CLASS_NAME)) {
      final Iterable<YTEntityImpl> result = database.browseClass(OScheduledEvent.CLASS_NAME);
      for (YTEntityImpl d : result) {
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

  public static void create(YTDatabaseSessionInternal database) {
    if (database
        .getMetadata()
        .getImmutableSchemaSnapshot()
        .existsClass(OScheduledEvent.CLASS_NAME)) {
      return;
    }
    final YTClass f = database.getMetadata().getSchema().createClass(OScheduledEvent.CLASS_NAME);
    f.createProperty(database, OScheduledEvent.PROP_NAME, YTType.STRING, (YTType) null, true)
        .setMandatory(database, true)
        .setNotNull(database, true);
    f.createIndex(database, OScheduledEvent.PROP_NAME + "Index", YTClass.INDEX_TYPE.UNIQUE,
        OScheduledEvent.PROP_NAME);
    f.createProperty(database, OScheduledEvent.PROP_RULE, YTType.STRING, (YTType) null, true)
        .setMandatory(database, true)
        .setNotNull(database, true);
    f.createProperty(database, OScheduledEvent.PROP_ARGUMENTS, YTType.EMBEDDEDMAP, (YTType) null,
        true);
    f.createProperty(database, OScheduledEvent.PROP_STATUS, YTType.STRING, (YTType) null, true);
    f.createProperty(database,
            OScheduledEvent.PROP_FUNC,
            YTType.LINK,
            database.getMetadata().getSchema().getClass(OFunction.CLASS_NAME), true)
        .setMandatory(database, true)
        .setNotNull(database, true);
    f.createProperty(database, OScheduledEvent.PROP_STARTTIME, YTType.DATETIME, (YTType) null,
        true);
  }

  public void initScheduleRecord(YTDatabaseSessionInternal session, YTEntityImpl doc) {
    String name = doc.field(OScheduledEvent.PROP_NAME);
    final OScheduledEvent event = getEvent(name);
    if (event != null && event.getDocument(session) != doc) {
      throw new YTDatabaseException(
          "Scheduled event with name '" + name + "' already exists in database");
    }
    doc.field(OScheduledEvent.PROP_STATUS, OScheduler.STATUS.STOPPED.name());
  }

  public void preHandleUpdateScheduleInTx(YTDatabaseSessionInternal session, YTEntityImpl doc) {
    try {
      final String schedulerName = doc.field(OScheduledEvent.PROP_NAME);
      OScheduledEvent event = getEvent(schedulerName);

      if (event != null) {
        // UPDATED EVENT
        final Set<String> dirtyFields = new HashSet<>(Arrays.asList(doc.getDirtyFields()));

        if (dirtyFields.contains(OScheduledEvent.PROP_NAME)) {
          throw new YTValidationException("Scheduled event cannot change name");
        }

        if (dirtyFields.contains(OScheduledEvent.PROP_RULE)) {
          // RULE CHANGED, STOP CURRENT EVENT AND RESCHEDULE IT
          var tx = session.getTransaction();

          @SuppressWarnings("unchecked")
          Set<YTRID> rids = (Set<YTRID>) tx.getCustomData(RIDS_OF_EVENTS_TO_RESCHEDULE_KEY);
          if (rids == null) {
            rids = new HashSet<>();
            tx.setCustomData(RIDS_OF_EVENTS_TO_RESCHEDULE_KEY, rids);
          }

          rids.add(doc.getIdentity());
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "Error on updating scheduled event", ex);
    }
  }

  public void postHandleUpdateScheduleAfterTxCommit(YTDatabaseSessionInternal session,
      YTEntityImpl doc) {
    try {
      var tx = session.getTransaction();
      @SuppressWarnings("unchecked")
      Set<YTRID> rids = (Set<YTRID>) tx.getCustomData(RIDS_OF_EVENTS_TO_RESCHEDULE_KEY);

      if (rids != null && rids.contains(doc.getIdentity())) {
        final String schedulerName = doc.field(OScheduledEvent.PROP_NAME);
        OScheduledEvent event = getEvent(schedulerName);

        if (event != null) {
          // RULE CHANGED, STOP CURRENT EVENT AND RESCHEDULE IT
          updateEvent(session, new OScheduledEvent(doc, session));
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "Error on updating scheduled event", ex);
    }
  }
}
