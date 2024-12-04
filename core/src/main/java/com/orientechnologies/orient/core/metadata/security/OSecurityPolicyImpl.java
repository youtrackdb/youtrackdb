package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import javax.annotation.Nonnull;

public class OSecurityPolicyImpl implements OSecurityPolicy {

  private YTEntity element;

  public OSecurityPolicyImpl(YTEntity element) {
    this.element = element;
  }

  @Override
  public YTRID getIdentity() {
    return element.getIdentity();
  }

  public YTEntity getElement(@Nonnull YTDatabaseSessionInternal db) {
    if (element.isUnloaded()) {
      element = db.bindToSession(element);
    }
    return element;
  }

  public void setElement(YTEntity element) {
    this.element = element;
  }

  public String getName(@Nonnull YTDatabaseSessionInternal db) {
    return getElement(db).getProperty("name");
  }

  public void setName(@Nonnull YTDatabaseSessionInternal db, String name) {
    getElement(db).setProperty("name", name);
  }

  public boolean isActive(@Nonnull YTDatabaseSessionInternal db) {
    return Boolean.TRUE.equals(this.getElement(db).getProperty("active"));
  }

  public void setActive(@Nonnull YTDatabaseSessionInternal db, Boolean active) {
    this.getElement(db).setProperty("active", active);
  }

  public String getCreateRule(@Nonnull YTDatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("create");
  }

  public void setCreateRule(@Nonnull YTDatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("create", rule);
  }

  public String getReadRule(@Nonnull YTDatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("read");
  }

  public void setReadRule(@Nonnull YTDatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("read", rule);
  }

  public String getBeforeUpdateRule(@Nonnull YTDatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("beforeUpdate");
  }

  public void setBeforeUpdateRule(@Nonnull YTDatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("beforeUpdate", rule);
  }

  public String getAfterUpdateRule(@Nonnull YTDatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("afterUpdate");
  }

  public void setAfterUpdateRule(@Nonnull YTDatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("afterUpdate", rule);
  }

  public String getDeleteRule(@Nonnull YTDatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("delete");
  }

  public void setDeleteRule(@Nonnull YTDatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("delete", rule);
  }

  public String getExecuteRule(@Nonnull YTDatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("execute");
  }

  public void setExecuteRule(@Nonnull YTDatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("execute", rule);
  }

  protected static void validatePredicate(String predicate) throws IllegalArgumentException {
    if (predicate == null || predicate.trim().isEmpty()) {
      return;
    }
    try {
      OSQLEngine.parsePredicate(predicate);
    } catch (OCommandSQLParsingException ex) {
      throw new IllegalArgumentException("Invalid predicate: " + predicate);
    }
  }
}
