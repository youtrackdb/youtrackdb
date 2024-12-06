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

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.ValidationException;
import com.jetbrains.youtrack.db.internal.core.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.metadata.function.Function;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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
public class SchedulerImpl {

  private static final String RIDS_OF_EVENTS_TO_RESCHEDULE_KEY =
      SchedulerImpl.class.getName() + ".ridsOfEventsToReschedule";

  private final ConcurrentHashMap<String, ScheduledEvent> events =
      new ConcurrentHashMap<>();

  private final YouTrackDBInternal youtrackDB;

  public SchedulerImpl(YouTrackDBInternal youtrackDB) {
    this.youtrackDB = youtrackDB;
  }

  public void scheduleEvent(DatabaseSession session, final ScheduledEvent event) {
    if (events.putIfAbsent(event.getName(session), event) == null) {
      String database = session.getName();
      event.schedule(database, "admin", youtrackDB);
    }
  }

  public ScheduledEvent removeEventInternal(final String eventName) {
    final ScheduledEvent event = events.remove(eventName);

    if (event != null) {
      event.interrupt();
    }
    return event;
  }

  public void removeEvent(DatabaseSessionInternal session, final String eventName) {
    LogManager.instance().debug(this, "Removing scheduled event '%s'...", eventName);

    final ScheduledEvent event = removeEventInternal(eventName);

    if (event != null) {
      try {
        session.load(event.getDocument(session).getIdentity());
      } catch (RecordNotFoundException ignore) {
        // ALREADY DELETED, JUST RETURN
        return;
      }

      // RECORD EXISTS: DELETE THE EVENT RECORD
      session.begin();
      event.getDocument(session).delete();
      session.commit();
    }
  }

  public void updateEvent(DatabaseSessionInternal session, final ScheduledEvent event) {
    final ScheduledEvent oldEvent = events.remove(event.getName(session));
    if (oldEvent != null) {
      oldEvent.interrupt();
    }
    scheduleEvent(session, event);
    LogManager.instance()
        .debug(
            this,
            "Updated scheduled event '%s' rid=%s...",
            event,
            event.getDocument(session).getIdentity());
  }

  public Map<String, ScheduledEvent> getEvents() {
    return events;
  }

  public ScheduledEvent getEvent(final String name) {
    return events.get(name);
  }

  public void load(DatabaseSessionInternal database) {
    if (database.getMetadata().getSchema().existsClass(ScheduledEvent.CLASS_NAME)) {
      final Iterable<EntityImpl> result = database.browseClass(ScheduledEvent.CLASS_NAME);
      for (EntityImpl d : result) {
        scheduleEvent(database, new ScheduledEvent(d, database));
      }
    }
  }

  public void close() {
    for (ScheduledEvent event : events.values()) {
      event.interrupt();
    }
    events.clear();
  }

  public static void create(DatabaseSessionInternal database) {
    if (database
        .getMetadata()
        .getImmutableSchemaSnapshot()
        .existsClass(ScheduledEvent.CLASS_NAME)) {
      return;
    }
    final SchemaClass f = database.getMetadata().getSchema().createClass(ScheduledEvent.CLASS_NAME);
    f.createProperty(database, ScheduledEvent.PROP_NAME, PropertyType.STRING, (PropertyType) null,
            true)
        .setMandatory(database, true)
        .setNotNull(database, true);
    f.createIndex(database, ScheduledEvent.PROP_NAME + "Index", SchemaClass.INDEX_TYPE.UNIQUE,
        ScheduledEvent.PROP_NAME);
    f.createProperty(database, ScheduledEvent.PROP_RULE, PropertyType.STRING, (PropertyType) null,
            true)
        .setMandatory(database, true)
        .setNotNull(database, true);
    f.createProperty(database, ScheduledEvent.PROP_ARGUMENTS, PropertyType.EMBEDDEDMAP,
        (PropertyType) null,
        true);
    f.createProperty(database, ScheduledEvent.PROP_STATUS, PropertyType.STRING, (PropertyType) null,
        true);
    f.createProperty(database,
            ScheduledEvent.PROP_FUNC,
            PropertyType.LINK,
            database.getMetadata().getSchema().getClass(Function.CLASS_NAME), true)
        .setMandatory(database, true)
        .setNotNull(database, true);
    f.createProperty(database, ScheduledEvent.PROP_STARTTIME, PropertyType.DATETIME,
        (PropertyType) null,
        true);
  }

  public void initScheduleRecord(DatabaseSessionInternal session, EntityImpl entity) {
    String name = entity.field(ScheduledEvent.PROP_NAME);
    final ScheduledEvent event = getEvent(name);
    if (event != null && event.getDocument(session) != entity) {
      throw new DatabaseException(
          "Scheduled event with name '" + name + "' already exists in database");
    }
    entity.field(ScheduledEvent.PROP_STATUS, Scheduler.STATUS.STOPPED.name());
  }

  public void preHandleUpdateScheduleInTx(DatabaseSessionInternal session, EntityImpl entity) {
    try {
      final String schedulerName = entity.field(ScheduledEvent.PROP_NAME);
      ScheduledEvent event = getEvent(schedulerName);

      if (event != null) {
        // UPDATED EVENT
        final Set<String> dirtyFields = new HashSet<>(Arrays.asList(entity.getDirtyFields()));

        if (dirtyFields.contains(ScheduledEvent.PROP_NAME)) {
          throw new ValidationException("Scheduled event cannot change name");
        }

        if (dirtyFields.contains(ScheduledEvent.PROP_RULE)) {
          // RULE CHANGED, STOP CURRENT EVENT AND RESCHEDULE IT
          var tx = session.getTransaction();

          @SuppressWarnings("unchecked")
          Set<RID> rids = (Set<RID>) tx.getCustomData(RIDS_OF_EVENTS_TO_RESCHEDULE_KEY);
          if (rids == null) {
            rids = new HashSet<>();
            tx.setCustomData(RIDS_OF_EVENTS_TO_RESCHEDULE_KEY, rids);
          }

          rids.add(entity.getIdentity());
        }
      }
    } catch (Exception ex) {
      LogManager.instance().error(this, "Error on updating scheduled event", ex);
    }
  }

  public void postHandleUpdateScheduleAfterTxCommit(DatabaseSessionInternal session,
      EntityImpl entity) {
    try {
      var tx = session.getTransaction();
      @SuppressWarnings("unchecked")
      Set<RID> rids = (Set<RID>) tx.getCustomData(RIDS_OF_EVENTS_TO_RESCHEDULE_KEY);

      if (rids != null && rids.contains(entity.getIdentity())) {
        final String schedulerName = entity.field(ScheduledEvent.PROP_NAME);
        ScheduledEvent event = getEvent(schedulerName);

        if (event != null) {
          // RULE CHANGED, STOP CURRENT EVENT AND RESCHEDULE IT
          updateEvent(session, new ScheduledEvent(entity, session));
        }
      }
    } catch (Exception ex) {
      LogManager.instance().error(this, "Error on updating scheduled event", ex);
    }
  }
}
