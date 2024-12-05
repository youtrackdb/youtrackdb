package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OToken;
import com.jetbrains.youtrack.db.internal.core.metadata.security.binary.OBinaryTokenSerializer;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 *
 */
public class OStorageRemoteNodeSession {

  private final String serverURL;
  private Integer sessionId = -1;
  private byte[] token = null;
  private OToken tokenInstance = null;

  public OStorageRemoteNodeSession(String serverURL, Integer uniqueClientSessionId) {
    this.serverURL = serverURL;
    this.sessionId = uniqueClientSessionId;
  }

  public String getServerURL() {
    return serverURL;
  }

  public Integer getSessionId() {
    return sessionId;
  }

  public byte[] getToken() {
    return token;
  }

  public void setSession(Integer sessionId, byte[] token) {
    this.sessionId = sessionId;
    this.token = token;
    if (token != null) {
      OBinaryTokenSerializer binarySerializer = new OBinaryTokenSerializer();
      try {
        this.tokenInstance = binarySerializer.deserialize(new ByteArrayInputStream(token));
      } catch (IOException e) {
        LogManager.instance().debug(this, "Error deserializing binary token", e);
      }
    }
  }

  public boolean isExpired() {
    if (this.tokenInstance != null) {
      return !this.tokenInstance.isNowValid();
    } else {
      return false;
    }
  }

  public boolean isValid() {
    return this.sessionId >= 0 && !isExpired();
  }
}
