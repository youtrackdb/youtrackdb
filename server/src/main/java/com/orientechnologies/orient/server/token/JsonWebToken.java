package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.YTUser;
import com.orientechnologies.orient.core.metadata.security.jwt.OJsonWebToken;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OrientJwtHeader;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;

/**
 *
 */
public class JsonWebToken implements OJsonWebToken, OToken {

  public final OTokenHeader header;
  public final OJwtPayload payload;
  private boolean isVerified;
  private boolean isValid;

  public JsonWebToken() {
    this(new OrientJwtHeader(), new OrientJwtPayload());
  }

  public JsonWebToken(OTokenHeader header, OJwtPayload payload) {
    isVerified = false;
    isValid = false;
    this.header = header;
    this.payload = payload;
  }

  @Override
  public OTokenHeader getHeader() {
    return header;
  }

  @Override
  public OJwtPayload getPayload() {
    return payload;
  }

  @Override
  public boolean getIsVerified() {
    return isVerified;
  }

  @Override
  public void setIsVerified(boolean verified) {
    this.isVerified = verified;
  }

  @Override
  public boolean getIsValid() {
    return this.isValid;
  }

  @Override
  public void setIsValid(boolean valid) {
    this.isValid = valid;
  }

  @Override
  public String getUserName() {
    return payload.getUserName();
  }

  @Override
  public String getDatabase() {
    return payload.getDatabase();
  }

  @Override
  public long getExpiry() {
    return payload.getExpiry();
  }

  @Override
  public YTRID getUserId() {
    return payload.getUserRid();
  }

  @Override
  public String getDatabaseType() {
    return payload.getDatabaseType();
  }

  @Override
  public YTUser getUser(YTDatabaseSessionInternal db) {
    YTRID userRid = payload.getUserRid();
    YTDocument result;
    result = db.load(userRid);
    if (!ODocumentInternal.getImmutableSchemaClass(result).isOuser()) {
      result = null;
    }
    return new YTUser(db, result);
  }

  @Override
  public void setExpiry(long expiry) {
    this.payload.setExpiry(expiry);
  }

  @Override
  public boolean isNowValid() {
    long now = System.currentTimeMillis();
    return getExpiry() > now && payload.getNotBefore() < now;
  }

  @Override
  public boolean isCloseToExpire() {
    long now = System.currentTimeMillis();
    return getExpiry() - 120000 <= now;
  }
}
