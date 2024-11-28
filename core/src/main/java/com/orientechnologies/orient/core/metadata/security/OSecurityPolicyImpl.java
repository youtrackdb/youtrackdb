package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import javax.annotation.Nonnull;

public class OSecurityPolicyImpl implements OSecurityPolicy {

  private OElement element;

  public OSecurityPolicyImpl(OElement element) {
    this.element = element;
  }

  @Override
  public ORID getIdentity() {
    return element.getIdentity();
  }

  public OElement getElement(@Nonnull ODatabaseSessionInternal db) {
    if (element.isUnloaded()) {
      element = db.bindToSession(element);
    }
    return element;
  }

  public void setElement(OElement element) {
    this.element = element;
  }

  public String getName(@Nonnull ODatabaseSessionInternal db) {
    return getElement(db).getProperty("name");
  }

  public void setName(@Nonnull ODatabaseSessionInternal db, String name) {
    getElement(db).setProperty("name", name);
  }

  public boolean isActive(@Nonnull ODatabaseSessionInternal db) {
    return Boolean.TRUE.equals(this.getElement(db).getProperty("active"));
  }

  public void setActive(@Nonnull ODatabaseSessionInternal db, Boolean active) {
    this.getElement(db).setProperty("active", active);
  }

  public String getCreateRule(@Nonnull ODatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("create");
  }

  public void setCreateRule(@Nonnull ODatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("create", rule);
  }

  public String getReadRule(@Nonnull ODatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("read");
  }

  public void setReadRule(@Nonnull ODatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("read", rule);
  }

  public String getBeforeUpdateRule(@Nonnull ODatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("beforeUpdate");
  }

  public void setBeforeUpdateRule(@Nonnull ODatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("beforeUpdate", rule);
  }

  public String getAfterUpdateRule(@Nonnull ODatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("afterUpdate");
  }

  public void setAfterUpdateRule(@Nonnull ODatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("afterUpdate", rule);
  }

  public String getDeleteRule(@Nonnull ODatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("delete");
  }

  public void setDeleteRule(@Nonnull ODatabaseSessionInternal db, String rule)
      throws IllegalArgumentException {
    validatePredicate(rule);
    getElement(db).setProperty("delete", rule);
  }

  public String getExecuteRule(@Nonnull ODatabaseSessionInternal db) {
    var element = getElement(db);
    return element == null ? null : element.getProperty("execute");
  }

  public void setExecuteRule(@Nonnull ODatabaseSessionInternal db, String rule)
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
