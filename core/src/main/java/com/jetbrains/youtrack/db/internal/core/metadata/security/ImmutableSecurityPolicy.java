package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import javax.annotation.Nonnull;

public class ImmutableSecurityPolicy implements SecurityPolicy {

  private final RID identity;
  private final String name;
  private final boolean active;
  private final String create;
  private final String read;
  private final String beforeUpdate;
  private final String afterUpdate;
  private final String delete;
  private final String execute;

  public ImmutableSecurityPolicy(@Nonnull DatabaseSessionInternal db, SecurityPolicy element) {
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

  public ImmutableSecurityPolicy(
      String name,
      String create,
      String read,
      String beforeUpdate,
      String afterUpdate,
      String delete,
      String execute) {
    super();
    this.identity = new RecordId(-1, -1);
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
  public String getName(@Nonnull DatabaseSessionInternal session) {
    return name;
  }

  @Override
  public boolean isActive(@Nonnull DatabaseSessionInternal session) {
    return active;
  }

  @Override
  public String getCreateRule(@Nonnull DatabaseSessionInternal session) {
    return create;
  }

  @Override
  public String getReadRule(@Nonnull DatabaseSessionInternal session) {
    return read;
  }

  @Override
  public String getBeforeUpdateRule(@Nonnull DatabaseSessionInternal session) {
    return beforeUpdate;
  }

  @Override
  public String getAfterUpdateRule(@Nonnull DatabaseSessionInternal session) {
    return afterUpdate;
  }

  @Override
  public String getDeleteRule(@Nonnull DatabaseSessionInternal session) {
    return delete;
  }

  @Override
  public String getExecuteRule(@Nonnull DatabaseSessionInternal session) {
    return execute;
  }

  @Override
  public RID getIdentity() {
    return identity;
  }
}
