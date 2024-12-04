package com.orientechnologies.orient.core.metadata.security.binary;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.jwt.OBinaryTokenPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import com.orientechnologies.orient.core.record.impl.YTDocument;

public class OBinaryToken implements OToken {

  private boolean valid;
  private boolean verified;
  private OTokenHeader header;
  private OBinaryTokenPayload payload;

  @Override
  public boolean getIsVerified() {
    return verified;
  }

  @Override
  public void setIsVerified(boolean verified) {
    this.verified = verified;
  }

  @Override
  public boolean getIsValid() {
    return valid;
  }

  @Override
  public void setIsValid(boolean valid) {
    this.valid = valid;
  }

  @Override
  public String getUserName() {
    return payload.getUserName();
  }

  @Override
  public OUser getUser(YTDatabaseSessionInternal db) {
    if (this.payload.getUserRid() != null) {
      try {
        YTDocument result = db.load(new YTRecordId(this.payload.getUserRid()));
        if (result.getClassName().equals(OUser.CLASS_NAME)) {
          return new OUser(db, result);
        }
      } catch (ORecordNotFoundException e) {
        return null;
      }
    }
    return null;
  }

  @Override
  public String getDatabase() {
    return this.payload.getDatabase();
  }

  @Override
  public String getDatabaseType() {
    return this.payload.getDatabaseType();
  }

  @Override
  public YTRID getUserId() {
    return this.payload.getUserRid();
  }

  public OTokenHeader getHeader() {
    return header;
  }

  public void setHeader(OTokenHeader header) {
    this.header = header;
  }

  @Override
  public void setExpiry(long expiry) {
    payload.setExpiry(expiry);
  }

  @Override
  public long getExpiry() {
    return payload.getExpiry();
  }

  public short getProtocolVersion() {
    return payload.getProtocolVersion();
  }

  public String getSerializer() {
    return payload.getSerializer();
  }

  public String getDriverName() {
    return payload.getDriverName();
  }

  public String getDriverVersion() {
    return payload.getDriverVersion();
  }

  public boolean isServerUser() {
    return payload.isServerUser();
  }

  @Override
  public boolean isNowValid() {
    long now = System.currentTimeMillis();
    return getExpiry() > now;
  }

  @Override
  public boolean isCloseToExpire() {
    long now = System.currentTimeMillis();
    return getExpiry() - 120000 <= now;
  }

  public OBinaryTokenPayload getPayload() {
    return payload;
  }

  public void setPayload(OBinaryTokenPayload payload) {
    this.payload = payload;
  }
}
