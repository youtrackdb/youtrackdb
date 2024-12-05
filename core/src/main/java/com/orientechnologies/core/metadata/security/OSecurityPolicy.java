package com.orientechnologies.core.metadata.security;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.id.YTRID;
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

  YTRID getIdentity();

  String getName(@Nonnull YTDatabaseSessionInternal db);

  boolean isActive(@Nonnull YTDatabaseSessionInternal db);

  String getCreateRule(@Nonnull YTDatabaseSessionInternal db);

  String getReadRule(@Nonnull YTDatabaseSessionInternal db);

  String getBeforeUpdateRule(@Nonnull YTDatabaseSessionInternal db);

  String getAfterUpdateRule(@Nonnull YTDatabaseSessionInternal db);

  String getDeleteRule(@Nonnull YTDatabaseSessionInternal db);

  String getExecuteRule(@Nonnull YTDatabaseSessionInternal db);

  default String get(Scope scope, @Nonnull YTDatabaseSessionInternal db) {
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
