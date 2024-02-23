package com.orientechnologies.common.log;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.event.Level;

/**
 * Centralized Log Manager used in OrientDB. All the log messages are routed through this class. It
 * uses SLF4J as the logging facade. Logging methods are accepting messages formatted as in {@link
 * String#format(String, Object...)} It is strongly recommended to use specialized logging methods
 * from this class instead of generic {@link #log(Object, Level, String, Throwable, Object...)}
 * method.
 */
public abstract class OSL4JLogManager {
  private final ConcurrentHashMap<String, Logger> loggersCache = new ConcurrentHashMap<>();

  protected static final String DEFAULT_LOG = "com.orientechnologies";
  protected boolean debug = false;
  protected boolean info = true;
  protected boolean warn = true;
  protected boolean error = true;

  /**
   * Loges a message if provided level of logging is enabled.
   *
   * @param requester the object that requested the log
   * @param level the level of the log
   * @param message the message to log, accepts format provided in {@link String#format(String,
   *     Object...)}
   * @param exception the exception to log
   * @param additionalArgs additional arguments to format the message
   */
  public void log(
      @Nonnull Object requester,
      @Nonnull Level level,
      @Nonnull String message,
      @Nullable Throwable exception,
      @Nullable Object... additionalArgs) {
    Objects.requireNonNull(requester);
    Objects.requireNonNull(level);
    Objects.requireNonNull(message);
    final String requesterName;
    if (requester instanceof Class<?>) {
      requesterName = ((Class<?>) requester).getName();
    } else {
      requesterName = requester.getClass().getName();
    }

    var log =
        loggersCache.compute(
            requesterName,
            (k, v) -> {
              if (v == null) {
                return LoggerFactory.getLogger(k);
              } else {
                return v;
              }
            });

    if (log.isEnabledForLevel(level)) {
      String dbName = fetchDbName(requester);

      Marker dbMarker = null;
      if (dbName != null) {
        message = "[" + dbName + "] " + message;
        dbMarker = MarkerFactory.getMarker("orientdb:" + dbName);
      }

      // USE THE LOG
      try {
        final String msg;
        if (additionalArgs != null && additionalArgs.length > 0) {
          msg = String.format(message, additionalArgs);
        } else {
          msg = message;
        }

        var logEventBuilder = log.makeLoggingEventBuilder(level);
        logEventBuilder = logEventBuilder.setMessage(msg);
        if (dbMarker != null) {
          logEventBuilder = logEventBuilder.addMarker(dbMarker);
        }

        logEventBuilder.log();
      } catch (Exception e) {
        System.err.println("Error on formatting message '" + message + "'. Exception: " + e);
      }
    }
  }

  private static String fetchDbName(Object requester) {
    String dbName = null;
    try {
      if (requester instanceof OStorage) {
        dbName = ((OStorage) requester).getName();
      } else {
        var dbInstance = ODatabaseRecordThreadLocal.getInstanceIfDefined();
        if (dbInstance != null) {
          var db = dbInstance.getIfDefined();
          if (db != null && db.getStorage() != null) {
            dbName = db.getStorage().getName();
          }
        }
      }
    } catch (Exception ignore) {
    }

    return dbName;
  }

  /**
   * Loges a message with debug level if this level of logging is enabled.
   *
   * @param requester the object that requested the log
   * @param message the message to log, accepts format provided in {@link String#format(String,
   *     Object...)}
   * @param additionalArgs additional arguments to format the message
   */
  public void debug(
      @Nonnull final Object requester,
      @Nonnull final String message,
      @Nullable final Object... additionalArgs) {
    debug(requester, message, null, additionalArgs);
  }

  /**
   * Loges a message with debug level if this level of logging is enabled.
   *
   * @param requester the object that requested the log
   * @param message the message to log, accepts format provided in {@link String#format(String,
   *     Object...)}
   * @param exception the exception to log
   * @param additionalArgs additional arguments to format the message
   */
  public void debug(
      @Nonnull final Object requester,
      @Nonnull final String message,
      @Nullable final Throwable exception,
      @Nullable final Object... additionalArgs) {
    if (isDebugEnabled()) {
      log(requester, Level.DEBUG, message, exception, additionalArgs);
    }
  }

  /**
   * Loges a message with info level if this level of logging is enabled.
   *
   * @param requester the object that requested the log
   * @param message the message to log, accepts format provided in {@link String#format(String,
   *     Object...)}
   * @param additionalArgs additional arguments to format the message
   */
  public void info(
      @Nonnull final Object requester,
      @Nonnull final String message,
      @Nullable final Object... additionalArgs) {
    info(requester, message, null, additionalArgs);
  }

  /**
   * Loges a message with info level if this level of logging is enabled.
   *
   * @param requester the object that requested the log
   * @param message the message to log, accepts format provided in {@link String#format(String,
   *     Object...)}
   * @param exception the exception to log
   * @param additionalArgs additional arguments to format the message
   */
  public void info(
      final @Nonnull Object requester,
      final @Nonnull String message,
      final @Nullable Throwable exception,
      final @Nullable Object... additionalArgs) {
    if (isInfoEnabled()) {
      log(requester, Level.INFO, message, exception, additionalArgs);
    }
  }

  /**
   * Loges a message with warn level if this level of logging is enabled.
   *
   * @param requester the object that requested the log
   * @param message the message to log, accepts format provided in {@link String#format(String,
   *     Object...)}
   * @param additionalArgs additional arguments to format the message
   */
  public void warn(
      @Nonnull final Object requester,
      @Nonnull final String message,
      @Nullable final Object... additionalArgs) {
    warn(requester, message, null, additionalArgs);
  }

  /**
   * Loges a message with warn level if this level of logging is enabled.
   *
   * @param requester the object that requested the log
   * @param message the message to log, accepts format provided in {@link String#format(String,
   *     Object...)}
   * @param exception the exception to log
   * @param additionalArgs additional arguments to format the message
   */
  public void warn(
      @Nonnull final Object requester,
      @Nonnull final String message,
      @Nullable final Throwable exception,
      @Nullable final Object... additionalArgs) {
    if (isWarnEnabled()) {
      log(requester, Level.WARN, message, exception, additionalArgs);
    }
  }

  /**
   * Loges a message with error level if this level of logging is enabled.
   *
   * @param requester the object that requested the log
   * @param message the message to log, accepts format provided in {@link String#format(String,
   *     Object...)}
   * @param additionalArgs additional arguments to format the message
   */
  public void error(
      @Nonnull final Object requester,
      @Nonnull final String message,
      @Nullable final Throwable exception,
      @Nullable final Object... additionalArgs) {
    if (isErrorEnabled()) {
      log(requester, Level.ERROR, message, exception, additionalArgs);
    }
  }

  public boolean isWarn() {
    return warn;
  }

  public OCommandOutputListener getCommandOutputListener(
      final Object requester, final Level level) {
    return text -> log(requester, level, text, null);
  }

  public boolean isDebugEnabled() {
    return debug;
  }

  public boolean isInfoEnabled() {
    return info;
  }

  public boolean isWarnEnabled() {
    return warn;
  }

  public boolean isErrorEnabled() {
    return error;
  }

  public void setErrorEnabled(boolean error) {
    this.error = error;
  }
}
