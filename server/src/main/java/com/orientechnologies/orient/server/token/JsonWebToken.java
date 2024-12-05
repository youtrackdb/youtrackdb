package com.orientechnologies.orient.server.token;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OToken;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTUser;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.OJsonWebToken;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.OJwtPayload;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.OTokenHeader;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.OrientJwtHeader;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;

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
    EntityImpl result;
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
