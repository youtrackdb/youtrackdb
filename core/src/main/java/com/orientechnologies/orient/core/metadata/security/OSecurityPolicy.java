package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.id.ORID;
import javax.annotation.Nonnull;

public interface OSecurityPolicy {

  enum Scope {
    CREATE,
    READ,
    BEFORE_UPDATE,
    AFTER_UPDATE,
    DELETE,
    EXECUTE
  }

  ORID getIdentity();

  String getName(@Nonnull ODatabaseSessionInternal db);

  boolean isActive(@Nonnull ODatabaseSessionInternal db);

  String getCreateRule(@Nonnull ODatabaseSessionInternal db);

  String getReadRule(@Nonnull ODatabaseSessionInternal db);

  String getBeforeUpdateRule(@Nonnull ODatabaseSessionInternal db);

  String getAfterUpdateRule(@Nonnull ODatabaseSessionInternal db);

  String getDeleteRule(@Nonnull ODatabaseSessionInternal db);

  String getExecuteRule(@Nonnull ODatabaseSessionInternal db);

  default String get(Scope scope, @Nonnull ODatabaseSessionInternal db) {
    return switch (scope) {
      case CREATE -> getCreateRule(db);
      case READ -> getReadRule(db);
      case BEFORE_UPDATE -> getBeforeUpdateRule(db);
      case AFTER_UPDATE -> getAfterUpdateRule(db);
      case DELETE -> getDeleteRule(db);
      case EXECUTE -> getExecuteRule(db);
      default -> throw new IllegalArgumentException();
    };
  }
}
