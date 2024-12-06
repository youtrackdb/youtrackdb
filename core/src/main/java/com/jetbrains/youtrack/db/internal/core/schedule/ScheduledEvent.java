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

import com.jetbrains.youtrack.db.internal.common.concur.NeedRetryException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandScriptException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseExportException;
import com.jetbrains.youtrack.db.internal.core.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.function.Function;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.schedule.Scheduler.STATUS;
import com.jetbrains.youtrack.db.internal.core.type.EntityWrapper;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

/**
 * Represents an instance of a scheduled event.
 *
 * @since Mar 28, 2013
 */
public class ScheduledEvent extends EntityWrapper {

  public static final String CLASS_NAME = "OSchedule";

  public static final String PROP_NAME = "name";
  public static final String PROP_RULE = "rule";
  public static final String PROP_ARGUMENTS = "arguments";
  public static final String PROP_STATUS = "status";
  public static final String PROP_FUNC = "function";
  public static final String PROP_STARTTIME = "starttime";
  public static final String PROP_EXEC_ID = "nextExecId";

  private Function function;
  private final AtomicBoolean running;
  private CronExpression cron;
  private volatile TimerTask timer;
  private final AtomicLong nextExecutionId;

  /**
   * Creates a scheduled event object from a configuration.
   */
  public ScheduledEvent(final EntityImpl doc, DatabaseSession session) {
    super(doc);
    running = new AtomicBoolean(false);
    nextExecutionId = new AtomicLong(getNextExecutionId(session));
    getFunction(session);
    try {
      cron = new CronExpression(getRule(session));
    } catch (ParseException e) {
      LogManager.instance()
          .error(this, "Error on compiling cron expression " + getRule(session), e);
    }
  }

  public void interrupt() {
    synchronized (this) {
      final TimerTask t = timer;
      timer = null;
      if (t != null) {
        t.cancel();
      }
    }
  }

  public Function getFunction(DatabaseSession session) {
    final Function fun = getFunctionSafe(session);
    if (fun == null) {
      throw new CommandScriptException("Function cannot be null");
    }
    return fun;
  }

  public String getRule(DatabaseSession session) {
    return getDocument(session).field(PROP_RULE);
  }

  public String getName(DatabaseSession session) {
    return getDocument(session).field(PROP_NAME);
  }

  public long getNextExecutionId(DatabaseSession session) {
    Long value = getDocument(session).field(PROP_EXEC_ID);
    return value != null ? value : 0;
  }

  public String getStatus(DatabaseSession session) {
    return getDocument(session).field(PROP_STATUS);
  }

  @Nonnull
  public Map<String, Object> getArguments(DatabaseSession session) {
    var value = getDocument(session).<Map<String, Object>>getProperty(PROP_ARGUMENTS);

    if (value == null) {
      return Collections.emptyMap();
    }

    return value;
  }

  public Date getStartTime(DatabaseSession session) {
    return getDocument(session).field(PROP_STARTTIME);
  }

  public boolean isRunning() {
    return this.running.get();
  }

  public ScheduledEvent schedule(String database, String user, YouTrackDBInternal youtrackDB) {
    if (isRunning()) {
      interrupt();
    }

    if (!getIdentity().isPersistent()) {
      throw new DatabaseExportException("Cannot schedule an unsaved event");
    }

    ScheduledTimerTask task = new ScheduledTimerTask(this, database, user, youtrackDB);
    task.schedule();

    timer = task;
    return this;
  }

  @Override
  public String toString() {
    var database = DatabaseRecordThreadLocal.instance().getIfDefined();
    if (database == null) {
      return "OSchedule [name:"
          + getName(database)
          + ",rule:"
          + getRule(database)
          + ",current status:"
          + getStatus(database)
          + ",func:"
          + getFunctionSafe(database)
          + ",started:"
          + getStartTime(database)
          + "]";
    }

    return super.toString();
  }

  @Override
  public void fromStream(DatabaseSessionInternal session, final EntityImpl iDocument) {
    super.fromStream(session, iDocument);
    try {
      cron.buildExpression(getRule(session));
    } catch (ParseException e) {
      LogManager.instance()
          .error(this, "Error on compiling cron expression " + getRule(session), e);
    }
  }

  private void setRunning(boolean running) {
    this.running.set(running);
  }

  private Function getFunctionSafe(DatabaseSession session) {
    var document = getDocument(session);
    if (function == null) {
      final Object funcDoc = document.field(PROP_FUNC);
      if (funcDoc != null) {
        if (funcDoc instanceof Function) {
          function = (Function) funcDoc;
          // OVERWRITE FUNCTION ID
          document.field(PROP_FUNC, function.getId(session));
        } else if (funcDoc instanceof EntityImpl) {
          function = new Function((EntityImpl) funcDoc);
        } else if (funcDoc instanceof RecordId) {
          function = new Function((RecordId) funcDoc);
        }
      }
    }
    return function;
  }

  private static class ScheduledTimerTask extends TimerTask {

    private final ScheduledEvent event;
    private final String database;
    private final String user;
    private final YouTrackDBInternal youTrackDBInternal;

    private ScheduledTimerTask(
        ScheduledEvent event, String database, String user,
        YouTrackDBInternal youTrackDBInternal) {
      this.event = event;
      this.database = database;
      this.user = user;
      this.youTrackDBInternal = youTrackDBInternal;
    }

    public void schedule() {
      synchronized (this) {
        event.nextExecutionId.incrementAndGet();
        Date now = new Date();
        long time = event.cron.getNextValidTimeAfter(now).getTime();
        long delay = time - now.getTime();
        youTrackDBInternal.scheduleOnce(this, delay);
      }
    }

    @Override
    public void run() {
      youTrackDBInternal.execute(
          database,
          user,
          db -> {
            runTask(db);
            return null;
          });
    }

    private void runTask(DatabaseSession db) {
      if (event.running.get()) {
        LogManager.instance()
            .error(
                this,
                "Error: The scheduled event '" + event.getName(db) + "' is already running",
                null);
        return;
      }

      if (event.function == null) {
        LogManager.instance()
            .error(
                this,
                "Error: The scheduled event '" + event.getName(db) + "' has no configured function",
                null);
        return;
      }

      try {
        event.setRunning(true);

        LogManager.instance()
            .info(
                this,
                "Checking for the execution of the scheduled event '%s' executionId=%d...",
                event.getName(db),
                event.nextExecutionId.get());
        try {
          boolean executeEvent = executeEvent(db);
          if (executeEvent) {
            LogManager.instance()
                .info(
                    this,
                    "Executing scheduled event '%s' executionId=%d...",
                    event.getName(db),
                    event.nextExecutionId.get());
            executeEventFunction(db);
          }

        } finally {
          event.setRunning(false);
        }
      } catch (Exception e) {
        LogManager.instance().error(this, "Error during execution of scheduled function", e);
      } finally {
        if (event.timer != null) {
          // RE-SCHEDULE THE NEXT EVENT
          event.schedule(database, user, youTrackDBInternal);
        }
      }
    }

    private boolean executeEvent(DatabaseSession db) {
      for (int retry = 0; retry < 10; ++retry) {
        try {
          if (isEventAlreadyExecuted(db)) {
            break;
          }

          db.begin();
          var eventDoc = event.getDocument(db);
          eventDoc.field(PROP_STATUS, STATUS.RUNNING);
          eventDoc.field(PROP_STARTTIME, System.currentTimeMillis());
          eventDoc.field(PROP_EXEC_ID, event.nextExecutionId.get());

          eventDoc.save();
          db.commit();

          // OK
          return true;
        } catch (NeedRetryException e) {
          // CONCURRENT UPDATE, PROBABLY EXECUTED BY ANOTHER SERVER
          if (isEventAlreadyExecuted(db)) {
            break;
          }

          LogManager.instance()
              .info(
                  this,
                  "Cannot change the status of the scheduled event '%s' executionId=%d, retry %d",
                  e,
                  event.getName(db),
                  event.nextExecutionId.get(),
                  retry);

        } catch (RecordNotFoundException e) {
          LogManager.instance()
              .info(
                  this,
                  "Scheduled event '%s' executionId=%d not found on database, removing event",
                  e,
                  event.getName(db),
                  event.nextExecutionId.get());
          event.interrupt();
          break;
        } catch (Exception e) {
          // SUSPEND EXECUTION
          LogManager.instance()
              .error(
                  this,
                  "Error during starting of scheduled event '%s' executionId=%d",
                  e,
                  event.getName(db),
                  event.nextExecutionId.get());

          event.interrupt();
          break;
        }
      }
      return false;
    }

    private void executeEventFunction(DatabaseSession session) {
      Object result = null;
      try {
        var context = new BasicCommandContext();
        context.setDatabase((DatabaseSessionInternal) session);

        result = session.computeInTx(
            () -> event.function.executeInContext(context, event.getArguments(session)));
      } finally {
        LogManager.instance()
            .info(
                this,
                "Scheduled event '%s' executionId=%d completed with result: %s",
                event.getName(session),
                event.nextExecutionId.get(),
                result);
        for (int retry = 0; retry < 10; ++retry) {
          session.executeInTx(
              () -> {
                var eventDoc = event.getDocument(session);
                try {
                  eventDoc.field(PROP_STATUS, STATUS.WAITING);
                  eventDoc.save();
                } catch (NeedRetryException e) {
                  eventDoc.unload();
                } catch (Exception e) {
                  LogManager.instance()
                      .error(this, "Error on saving status for event '%s'", e,
                          event.getName(session));
                }
              });
        }
      }
    }

    private boolean isEventAlreadyExecuted(@Nonnull DatabaseSession session) {
      final Record rec;
      try {
        rec = event.getDocument(session).getIdentity().getRecord();
      } catch (RecordNotFoundException e) {
        return true;
      }

      final EntityImpl updated = session.load(rec.getIdentity());
      final Long currentExecutionId = updated.field(PROP_EXEC_ID);
      if (currentExecutionId == null) {
        return false;
      }

      if (currentExecutionId >= event.nextExecutionId.get()) {
        LogManager.instance()
            .info(
                this,
                "Scheduled event '%s' with id %d is already running (current id=%d)",
                event.getName(session),
                event.nextExecutionId.get(),
                currentExecutionId);
        // ALREADY RUNNING
        return true;
      }
      return false;
    }
  }
}
