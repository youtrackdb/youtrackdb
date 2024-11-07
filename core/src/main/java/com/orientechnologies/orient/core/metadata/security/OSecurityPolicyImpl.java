package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;

public class OSecurityPolicyImpl implements OSecurityPolicy {

  private OElement element;

  public OSecurityPolicyImpl(OElement element) {
    this.element = element;
  }

  @Override
  public ORID getIdentity() {
    return element.getIdentity();
  }

  public OElement getElement() {
    if (element.isUnloaded()) {
      element = ODatabaseSession.getActiveSession().bindToSession(element);
    }
    return element;
  }

  public void setElement(OElement element) {
    this.element = element;
  }

  public String getName() {
    return getElement().getProperty("name");
  }

  public void setName(String name) {
    getElement().setProperty("name", name);
  }

  public boolean isActive() {
    return Boolean.TRUE.equals(this.getElement().getProperty("active"));
  }

  public void setActive(Boolean active) {
    this.getElement().setProperty("active", active);
  }

  public String getCreateRule() {
    var element = getElement();
    return element == null ? null : element.getProperty("create");
  }

  public void setCreateRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    getElement().setProperty("create", rule);
  }

  public String getReadRule() {
    var element = getElement();
    return element == null ? null : element.getProperty("read");
  }

  public void setReadRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    getElement().setProperty("read", rule);
  }

  public String getBeforeUpdateRule() {
    var element = getElement();
    return element == null ? null : element.getProperty("beforeUpdate");
  }

  public void setBeforeUpdateRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    getElement().setProperty("beforeUpdate", rule);
  }

  public String getAfterUpdateRule() {
    var element = getElement();
    return element == null ? null : element.getProperty("afterUpdate");
  }

  public void setAfterUpdateRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    getElement().setProperty("afterUpdate", rule);
  }

  public String getDeleteRule() {
    var element = getElement();
    return element == null ? null : element.getProperty("delete");
  }

  public void setDeleteRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    getElement().setProperty("delete", rule);
  }

  public String getExecuteRule() {
    var element = getElement();
    return element == null ? null : element.getProperty("execute");
  }

  public void setExecuteRule(String rule) throws IllegalArgumentException {
    validatePredicate(rule);
    getElement().setProperty("execute", rule);
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
