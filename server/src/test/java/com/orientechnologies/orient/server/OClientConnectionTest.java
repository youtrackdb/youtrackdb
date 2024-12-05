package com.orientechnologies.orient.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.config.YTContextConfiguration;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.YTTokenSecurityException;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.server.token.OTokenHandlerImpl;
import java.io.IOException;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 *
 */
public class OClientConnectionTest extends BaseMemoryInternalDatabase {

  @Mock
  private ONetworkProtocolBinary protocol;

  @Mock
  private ONetworkProtocolBinary protocol1;

  @Mock
  private OClientConnectionManager manager;

  @Mock
  private OServer server;

  public void beforeTest() throws Exception {
    super.beforeTest();
    MockitoAnnotations.initMocks(this);
    Mockito.when(protocol.getServer()).thenReturn(server);
    Mockito.when(server.getClientConnectionManager()).thenReturn(manager);
    Mockito.when(server.getContextConfiguration()).thenReturn(new YTContextConfiguration());
  }

  @Test
  public void testValidToken() throws IOException {
    OClientConnection conn = new OClientConnection(1, protocol);
    OTokenHandler handler = new OTokenHandlerImpl(server.getContextConfiguration());
    byte[] tokenBytes = handler.getSignedBinaryToken(db, db.getUser(), conn.getData());

    conn.validateSession(tokenBytes, handler, null);
    assertTrue(conn.getTokenBased());
    assertArrayEquals(tokenBytes, conn.getTokenBytes());
    assertNotNull(conn.getToken());
  }

  @Test(expected = YTTokenSecurityException.class)
  public void testExpiredToken() throws IOException, InterruptedException {
    OClientConnection conn = new OClientConnection(1, protocol);
    long sessionTimeout = YTGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT.getValueAsLong();
    YTGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT.setValue(0);
    OTokenHandler handler = new OTokenHandlerImpl(server.getContextConfiguration());
    YTGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT.setValue(sessionTimeout);
    byte[] tokenBytes = handler.getSignedBinaryToken(db, db.getUser(), conn.getData());
    Thread.sleep(1);
    conn.validateSession(tokenBytes, handler, protocol);
  }

  @Test(expected = YTTokenSecurityException.class)
  public void testWrongToken() throws IOException {
    OClientConnection conn = new OClientConnection(1, protocol);
    OTokenHandler handler = new OTokenHandlerImpl(server.getContextConfiguration());
    byte[] tokenBytes = new byte[120];
    conn.validateSession(tokenBytes, handler, protocol);
  }

  @Test
  public void testAlreadyAuthenticatedOnConnection() throws IOException {
    OClientConnection conn = new OClientConnection(1, protocol);
    OTokenHandler handler = new OTokenHandlerImpl(server.getContextConfiguration());
    byte[] tokenBytes = handler.getSignedBinaryToken(db, db.getUser(), conn.getData());
    conn.validateSession(tokenBytes, handler, protocol);
    assertTrue(conn.getTokenBased());
    assertArrayEquals(tokenBytes, conn.getTokenBytes());
    assertNotNull(conn.getToken());
    // second validation don't need token
    conn.validateSession(null, handler, protocol);
    assertTrue(conn.getTokenBased());
    assertEquals(tokenBytes, conn.getTokenBytes());
    assertNotNull(conn.getToken());
  }

  @Test(expected = YTTokenSecurityException.class)
  public void testNotAlreadyAuthenticated() throws IOException {
    OClientConnection conn = new OClientConnection(1, protocol);
    OTokenHandler handler = new OTokenHandlerImpl(server.getContextConfiguration());
    // second validation don't need token
    conn.validateSession(null, handler, protocol1);
  }

  @Test(expected = YTTokenSecurityException.class)
  public void testAlreadyAuthenticatedButNotOnSpecificConnection() throws IOException {
    OClientConnection conn = new OClientConnection(1, protocol);
    OTokenHandler handler = new OTokenHandlerImpl(server.getContextConfiguration());
    byte[] tokenBytes = handler.getSignedBinaryToken(db, db.getUser(), conn.getData());
    conn.validateSession(tokenBytes, handler, protocol);
    assertTrue(conn.getTokenBased());
    assertArrayEquals(tokenBytes, conn.getTokenBytes());
    assertNotNull(conn.getToken());
    // second validation don't need token
    ONetworkProtocolBinary otherConn = Mockito.mock(ONetworkProtocolBinary.class);
    conn.validateSession(null, handler, otherConn);
  }
}
