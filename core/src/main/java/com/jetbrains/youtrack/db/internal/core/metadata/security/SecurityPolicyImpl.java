package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQLParsingException;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import javax.annotation.Nonnull;

public class SecurityPolicyImpl implements SecurityPolicy {

  private Entity element;

  public SecurityPolicyImpl(Entity element) {
    this.element = element;
  }

  @Override
  public RID getIdentity() {
    return element.getIdentity();
  }

  public Entity getElement(@Nonnull DatabaseSessionInternal db) {
    if (element.isUnloaded()) {
      element = db.bindToSession(element);
    }
    return element;
  }

  public void setElement(Entity element) {
    this.element = element;
  }

  public String getName(@Nonnull DatabaseSessionInternal db) {
    return getElement(db).getProperty("name");
  }

  public void setName(@Nonnull DatabaseSessionInternal db, String name) {
    getElement(db).setProperty("name", name);
  }

  public boolean isActive(@Nonnull DatabaseSessionInternal db) {
    return Boolean.TRUE.equals(this.getElement(db).getProperty("active"));
  }

  public void setActive(@Nonnull DatabaseSessionInternal db, Boolean active) {
    this.getElement(db).setProperty("active", active);
  }

  public String getCreateRule(@Nonnull DatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("create");
  }

  public void setCreateRule(@Nonnull DatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("create", rule);
  }

  public String getReadRule(@Nonnull DatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("read");
  }

  public void setReadRule(@Nonnull DatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("read", rule);
  }

  public String getBeforeUpdateRule(@Nonnull DatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("beforeUpdate");
  }

  public void setBeforeUpdateRule(@Nonnull DatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("beforeUpdate", rule);
  }

  public String getAfterUpdateRule(@Nonnull DatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("afterUpdate");
  }

  public void setAfterUpdateRule(@Nonnull DatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("afterUpdate", rule);
  }

  public String getDeleteRule(@Nonnull DatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("delete");
  }

  public void setDeleteRule(@Nonnull DatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("delete", rule);
  }

  public String getExecuteRule(@Nonnull DatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("execute");
  }

  public void setExecuteRule(@Nonnull DatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("execute", rule);
  }

  protected static void validatePredicate(String predicate) throws IllegalArgumentException {
    if (predicate == null || predicate.trim().isEmpty()) {
      return;
    }
    try {
      SQLEngine.parsePredicate(predicate);
    } catch (CommandSQLParsingException ex) {
      throw new IllegalArgumentException("Invalid predicate: " + predicate);
    }
  }
}
