package com.jetbrains.youtrack.db.internal.core;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public final class YouTrackDBScheduler {

  private final Timer timer = new Timer(true);
  private volatile boolean active = false;

  public void activate() {
    active = true;
  }

  public void shutdown() {
    active = false;
    timer.cancel();
  }

  public TimerTask scheduleTask(final Runnable task, final long delay, final long period) {
    return scheduleTask(task, new Date(System.currentTimeMillis() + delay), period);
  }

  public TimerTask scheduleTask(final Runnable task, final Date firstTime, final long period) {
    final TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        try {
          task.run();
        } catch (Exception e) {
          LogManager.instance()
              .error(
                  this,
                  "Error during execution of task " + task.getClass().getSimpleName(),
                  e);
        } catch (Error e) {
          LogManager.instance()
              .error(
                  this,
                  "Error during execution of task " + task.getClass().getSimpleName(),
                  e);
          throw e;
        }
      }
    };

    if (active) {
      if (period > 0) {
        timer.schedule(timerTask, firstTime, period);
      } else {
        timer.schedule(timerTask, firstTime);
      }
    } else {
      LogManager.instance().warn(this, "YouTrackDB engine is down. Task will not be scheduled.");
    }

    return timerTask;
  }
}
