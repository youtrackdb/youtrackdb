package com.jetbrains.youtrack.db.internal.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;

import com.jetbrains.youtrack.db.api.config.ContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.command.CommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.query.live.LiveQueryHook;
import com.jetbrains.youtrack.db.internal.core.query.live.LiveQueryListener;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkBase;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.SocketChannelBinaryServer;
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.LiveCommandResultListener;
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.NetworkProtocolBinary;
import com.jetbrains.youtrack.db.internal.server.token.TokenHandlerImpl;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 *
 */
public class LiveCommandResultListenerTest extends BaseMemoryInternalDatabase {

  @Mock
  private YouTrackDBServer server;
  @Mock
  private SocketChannelBinaryServer channelBinary;

  @Mock
  private LiveQueryListener rawListener;

  private NetworkProtocolBinary protocol;
  private ClientConnection connection;

  private static class TestResultListener implements CommandResultListener {

    @Override
    public boolean result(DatabaseSessionInternal db, Object iRecord) {
      return false;
    }

    @Override
    public void end() {
    }

    @Override
    public Object getResult() {
      return null;
    }
  }

  @Before
  public void beforeTests() {
    MockitoAnnotations.initMocks(this);
    Mockito.when(server.getContextConfiguration()).thenReturn(new ContextConfiguration());

    var manager = new ClientConnectionManager(server);
    protocol = new NetworkProtocolBinary(server);
    protocol.initVariables(server, channelBinary);
    connection = manager.connect(protocol);
    var tokenHandler = new TokenHandlerImpl(new ContextConfiguration());
    Mockito.when(server.getTokenHandler()).thenReturn(tokenHandler);
    var token = tokenHandler.getSignedBinaryToken(db, db.geCurrentUser(), connection.getData());
    connection = manager.connect(protocol, connection, token);
    connection.setDatabase(db);
    connection.getData().setSerializationImpl(RecordSerializerNetworkBase.NAME);
    Mockito.when(server.getClientConnectionManager()).thenReturn(manager);
  }

  @Test
  public void testSimpleMessageSend() throws IOException {
    var listener =
        new LiveCommandResultListener(server, connection, new TestResultListener());
    var op = new RecordOperation(new EntityImpl(db), RecordOperation.CREATED);
    listener.onLiveResult(db, 10, op);
    Mockito.verify(channelBinary, atLeastOnce()).writeBytes(Mockito.any(byte[].class));
  }

  @Test
  public void testNetworkError() throws IOException {
    Mockito.when(channelBinary.writeInt(Mockito.anyInt()))
        .thenThrow(new IOException("Mock Exception"));
    var listener =
        new LiveCommandResultListener(server, connection, new TestResultListener());
    LiveQueryHook.subscribe(10, rawListener, db);
    assertTrue(LiveQueryHook.getOpsReference(db).getQueueThread().hasToken(10));
    var op = new RecordOperation(new EntityImpl(db), RecordOperation.CREATED);
    listener.onLiveResult(db, 10, op);
    assertFalse(LiveQueryHook.getOpsReference(db).getQueueThread().hasToken(10));
  }
}
