package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.common.concur.resource.ResourcePool;
import com.jetbrains.youtrack.db.internal.common.concur.resource.ResourcePoolListener;
import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.io.YTIOException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.orientechnologies.orient.client.binary.SocketChannelBinaryAsynchClient;

/**
 *
 */
public class RemoteConnectionPool
    implements ResourcePoolListener<String, SocketChannelBinaryAsynchClient> {

  private final ResourcePool<String, SocketChannelBinaryAsynchClient> pool;

  public RemoteConnectionPool(int iMaxResources) {
    pool = new ResourcePool<>(iMaxResources, this);
  }

  protected SocketChannelBinaryAsynchClient createNetworkConnection(
      String serverURL, final ContextConfiguration clientConfiguration) throws YTIOException {
    if (serverURL == null) {
      throw new IllegalArgumentException("server url is null");
    }

    // TRY WITH CURRENT URL IF ANY
    try {
      LogManager.instance().debug(this, "Trying to connect to the remote host %s...", serverURL);

      int sepPos = serverURL.indexOf(':');
      final String remoteHost = serverURL.substring(0, sepPos);
      final int remotePort = Integer.parseInt(serverURL.substring(sepPos + 1));

      final SocketChannelBinaryAsynchClient ch =
          new SocketChannelBinaryAsynchClient(
              remoteHost,
              remotePort,
              clientConfiguration,
              ChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);

      return ch;

    } catch (YTIOException e) {
      // RE-THROW IT
      throw e;
    } catch (Exception e) {
      LogManager.instance().debug(this, "Error on connecting to %s", e, serverURL);
      throw BaseException.wrapException(new YTIOException("Error on connecting to " + serverURL),
          e);
    }
  }

  @Override
  public SocketChannelBinaryAsynchClient createNewResource(
      final String iKey, final Object... iAdditionalArgs) {
    return createNetworkConnection(iKey, (ContextConfiguration) iAdditionalArgs[0]);
  }

  @Override
  public boolean reuseResource(
      final String iKey, final Object[] iAdditionalArgs,
      final SocketChannelBinaryAsynchClient iValue) {
    final boolean canReuse = iValue.isConnected();
    if (!canReuse)
    // CANNOT REUSE: CLOSE IT PROPERLY
    {
      try {
        iValue.close();
      } catch (Exception e) {
        LogManager.instance().debug(this, "Error on closing socket connection", e);
      }
    }
    iValue.markInUse();
    return canReuse;
  }

  public ResourcePool<String, SocketChannelBinaryAsynchClient> getPool() {
    return pool;
  }

  public SocketChannelBinaryAsynchClient acquire(
      final String iServerURL,
      final long timeout,
      final ContextConfiguration clientConfiguration) {
    return pool.getResource(iServerURL, timeout, clientConfiguration);
  }

  public void checkIdle(long timeout) {
    for (SocketChannelBinaryAsynchClient resource : pool.getResources()) {
      if (!resource.isInUse() && resource.getLastUse() + timeout < System.currentTimeMillis()) {
        resource.close();
      }
    }
  }
}
