package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import javax.annotation.Nonnull;

public class OImmutableSecurityPolicy implements OSecurityPolicy {

  private final ORID identity;
  private final String name;
  private final boolean active;
  private final String create;
  private final String read;
  private final String beforeUpdate;
  private final String afterUpdate;
  private final String delete;
  private final String execute;

  public OImmutableSecurityPolicy(@Nonnull ODatabaseSessionInternal db, OSecurityPolicy element) {
    this.identity = element.getIdentity();
    this.name = element.getName(db);
    this.active = element.isActive(db);
    this.create = element.getCreateRule(db);
    this.read = element.getReadRule(db);
    this.beforeUpdate = element.getBeforeUpdateRule(db);
    this.afterUpdate = element.getAfterUpdateRule(db);
    this.delete = element.getDeleteRule(db);
    this.execute = element.getExecuteRule(db);
  }

  public OImmutableSecurityPolicy(
      String name,
      String create,
      String read,
      String beforeUpdate,
      String afterUpdate,
      String delete,
      String execute) {
    super();
    this.identity = new ORecordId(-1, -1);
    this.active = true;
    this.name = name;
    this.create = create;
    this.read = read;
    this.beforeUpdate = beforeUpdate;
    this.afterUpdate = afterUpdate;
    this.delete = delete;
    this.execute = execute;
  }

  @Override
  public String getName(@Nonnull ODatabaseSessionInternal db) {
    return name;
  }

  @Override
  public boolean isActive(@Nonnull ODatabaseSessionInternal db) {
    return active;
  }

  @Override
  public String getCreateRule(@Nonnull ODatabaseSessionInternal db) {
    return create;
  }

  @Override
  public String getReadRule(@Nonnull ODatabaseSessionInternal db) {
    return read;
  }

  @Override
  public String getBeforeUpdateRule(@Nonnull ODatabaseSessionInternal db) {
    return beforeUpdate;
  }

  @Override
  public String getAfterUpdateRule(@Nonnull ODatabaseSessionInternal db) {
    return afterUpdate;
  }

  @Override
  public String getDeleteRule(@Nonnull ODatabaseSessionInternal db) {
    return delete;
  }

  @Override
  public String getExecuteRule(@Nonnull ODatabaseSessionInternal db) {
    return execute;
  }

  @Override
  public ORID getIdentity() {
    return identity;
  }
}
