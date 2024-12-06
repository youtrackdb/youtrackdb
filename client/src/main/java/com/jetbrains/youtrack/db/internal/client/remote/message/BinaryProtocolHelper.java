package com.jetbrains.youtrack.db.internal.client.remote.message;

import static com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration.NETWORK_BINARY_MIN_PROTOCOL_VERSION;
import static com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol.OLDEST_SUPPORTED_PROTOCOL_VERSION;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;

public class BinaryProtocolHelper {

  public static void checkProtocolVersion(Object caller, int protocolVersion) {

    if (OLDEST_SUPPORTED_PROTOCOL_VERSION > protocolVersion) {
      String message =
          String.format(
              "Backward compatibility support available from to version %d your version is %d",
              OLDEST_SUPPORTED_PROTOCOL_VERSION, protocolVersion);
      LogManager.instance().error(caller, message, null);
      throw new DatabaseException(message);
    }

    if (NETWORK_BINARY_MIN_PROTOCOL_VERSION.getValueAsInteger() > protocolVersion) {
      String message =
          String.format(
              "Backward compatibility support enabled from version %d your version is %d, check"
                  + " `%s` settings",
              NETWORK_BINARY_MIN_PROTOCOL_VERSION.getValueAsInteger(),
              protocolVersion,
              NETWORK_BINARY_MIN_PROTOCOL_VERSION.getKey());
      LogManager.instance().error(caller, message, null);
      throw new DatabaseException(message);
    }
  }
}
