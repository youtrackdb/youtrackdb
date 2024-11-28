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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDBInternal;
import com.orientechnologies.orient.core.db.tool.ODatabaseExportException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OScheduler.STATUS;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
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
public class OScheduledEvent extends ODocumentWrapper {

  public static final String CLASS_NAME = "OSchedule";

  public static final String PROP_NAME = "name";
  public static final String PROP_RULE = "rule";
  public static final String PROP_ARGUMENTS = "arguments";
  public static final String PROP_STATUS = "status";
  public static final String PROP_FUNC = "function";
  public static final String PROP_STARTTIME = "starttime";
  public static final String PROP_EXEC_ID = "nextExecId";

  private OFunction function;
  private final AtomicBoolean running;
  private OCronExpression cron;
  private volatile TimerTask timer;
  private final AtomicLong nextExecutionId;

  /**
   * Creates a scheduled event object from a configuration.
   */
  public OScheduledEvent(final ODocument doc, ODatabaseSession session) {
    super(doc);
    running = new AtomicBoolean(false);
    nextExecutionId = new AtomicLong(getNextExecutionId(session));
    getFunction(session);
    try {
      cron = new OCronExpression(getRule(session));
    } catch (ParseException e) {
      OLogManager.instance()
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

  public OFunction getFunction(ODatabaseSession session) {
    final OFunction fun = getFunctionSafe(session);
    if (fun == null) {
      throw new OCommandScriptException("Function cannot be null");
    }
    return fun;
  }

  public String getRule(ODatabaseSession session) {
    return getDocument(session).field(PROP_RULE);
  }

  public String getName(ODatabaseSession session) {
    return getDocument(session).field(PROP_NAME);
  }

  public long getNextExecutionId(ODatabaseSession session) {
    Long value = getDocument(session).field(PROP_EXEC_ID);
    return value != null ? value : 0;
  }

  public String getStatus(ODatabaseSession session) {
    return getDocument(session).field(PROP_STATUS);
  }

  @Nonnull
  public Map<String, Object> getArguments(ODatabaseSession session) {
    var value = getDocument(session).<Map<String, Object>>getProperty(PROP_ARGUMENTS);

    if (value == null) {
      return Collections.emptyMap();
    }

    return value;
  }

  public Date getStartTime(ODatabaseSession session) {
    return getDocument(session).field(PROP_STARTTIME);
  }

  public boolean isRunning() {
    return this.running.get();
  }

  public OScheduledEvent schedule(String database, String user, OxygenDBInternal oxygenDB) {
    if (isRunning()) {
      interrupt();
    }

    if (!getIdentity().isPersistent()) {
      throw new ODatabaseExportException("Cannot schedule an unsaved event");
    }

    ScheduledTimerTask task = new ScheduledTimerTask(this, database, user, oxygenDB);
    task.schedule();

    timer = task;
    return this;
  }

  @Override
  public String toString() {
    var database = ODatabaseRecordThreadLocal.instance().getIfDefined();
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
  public void fromStream(ODatabaseSessionInternal session, final ODocument iDocument) {
    super.fromStream(session, iDocument);
    try {
      cron.buildExpression(getRule(session));
    } catch (ParseException e) {
      OLogManager.instance()
          .error(this, "Error on compiling cron expression " + getRule(session), e);
    }
  }

  private void setRunning(boolean running) {
    this.running.set(running);
  }

  private OFunction getFunctionSafe(ODatabaseSession session) {
    var document = getDocument(session);
    if (function == null) {
      final Object funcDoc = document.field(PROP_FUNC);
      if (funcDoc != null) {
        if (funcDoc instanceof OFunction) {
          function = (OFunction) funcDoc;
          // OVERWRITE FUNCTION ID
          document.field(PROP_FUNC, function.getId(session));
        } else if (funcDoc instanceof ODocument) {
          function = new OFunction((ODocument) funcDoc);
        } else if (funcDoc instanceof ORecordId) {
          function = new OFunction((ORecordId) funcDoc);
        }
      }
    }
    return function;
  }

  private static class ScheduledTimerTask extends TimerTask {

    private final OScheduledEvent event;
    private final String database;
    private final String user;
    private final OxygenDBInternal oxygenDB;

    private ScheduledTimerTask(
        OScheduledEvent event, String database, String user, OxygenDBInternal oxygenDB) {
      this.event = event;
      this.database = database;
      this.user = user;
      this.oxygenDB = oxygenDB;
    }

    public void schedule() {
      synchronized (this) {
        event.nextExecutionId.incrementAndGet();
        Date now = new Date();
        long time = event.cron.getNextValidTimeAfter(now).getTime();
        long delay = time - now.getTime();
        oxygenDB.scheduleOnce(this, delay);
      }
    }

    @Override
    public void run() {
      oxygenDB.execute(
          database,
          user,
          db -> {
            runTask(db);
            return null;
          });
    }

    private void runTask(ODatabaseSession db) {
      if (event.running.get()) {
        OLogManager.instance()
            .error(
                this,
                "Error: The scheduled event '" + event.getName(db) + "' is already running",
                null);
        return;
      }

      if (event.function == null) {
        OLogManager.instance()
            .error(
                this,
                "Error: The scheduled event '" + event.getName(db) + "' has no configured function",
                null);
        return;
      }

      try {
        event.setRunning(true);

        OLogManager.instance()
            .info(
                this,
                "Checking for the execution of the scheduled event '%s' executionId=%d...",
                event.getName(db),
                event.nextExecutionId.get());
        try {
          boolean executeEvent = executeEvent(db);
          if (executeEvent) {
            OLogManager.instance()
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
        OLogManager.instance().error(this, "Error during execution of scheduled function", e);
      } finally {
        if (event.timer != null) {
          // RE-SCHEDULE THE NEXT EVENT
          event.schedule(database, user, oxygenDB);
        }
      }
    }

    private boolean executeEvent(ODatabaseSession db) {
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
        } catch (ONeedRetryException e) {
          // CONCURRENT UPDATE, PROBABLY EXECUTED BY ANOTHER SERVER
          if (isEventAlreadyExecuted(db)) {
            break;
          }

          OLogManager.instance()
              .info(
                  this,
                  "Cannot change the status of the scheduled event '%s' executionId=%d, retry %d",
                  e,
                  event.getName(db),
                  event.nextExecutionId.get(),
                  retry);

        } catch (ORecordNotFoundException e) {
          OLogManager.instance()
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
          OLogManager.instance()
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

    private void executeEventFunction(ODatabaseSession session) {
      Object result = null;
      try {
        var context = new OBasicCommandContext();
        context.setDatabase((ODatabaseSessionInternal) session);

        result = session.computeInTx(
            () -> event.function.executeInContext(context, event.getArguments(session)));
      } finally {
        OLogManager.instance()
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
                } catch (ONeedRetryException e) {
                  eventDoc.unload();
                } catch (Exception e) {
                  OLogManager.instance()
                      .error(this, "Error on saving status for event '%s'", e,
                          event.getName(session));
                }
              });
        }
      }
    }

    private boolean isEventAlreadyExecuted(@Nonnull ODatabaseSession session) {
      final ORecord rec;
      try {
        rec = event.getDocument(session).getIdentity().getRecord();
      } catch (ORecordNotFoundException e) {
        return true;
      }

      final ODocument updated = session.load(rec.getIdentity());
      final Long currentExecutionId = updated.field(PROP_EXEC_ID);
      if (currentExecutionId == null) {
        return false;
      }

      if (currentExecutionId >= event.nextExecutionId.get()) {
        OLogManager.instance()
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
