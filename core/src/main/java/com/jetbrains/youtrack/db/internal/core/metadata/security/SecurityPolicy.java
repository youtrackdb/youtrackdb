package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.RID;
import javax.annotation.Nonnull;

public interface SecurityPolicy {

  String CLASS_NAME = "OSecurityPolicy";

  enum Scope {
    CREATE,
    READ,
    BEFORE_UPDATE,
    AFTER_UPDATE,
    DELETE,
    EXECUTE
  }

  RID getIdentity();

  String getName(@Nonnull DatabaseSessionInternal db);

  boolean isActive(@Nonnull DatabaseSessionInternal db);

  String getCreateRule(@Nonnull DatabaseSessionInternal db);

  String getReadRule(@Nonnull DatabaseSessionInternal db);

  String getBeforeUpdateRule(@Nonnull DatabaseSessionInternal db);

  String getAfterUpdateRule(@Nonnull DatabaseSessionInternal db);

  String getDeleteRule(@Nonnull DatabaseSessionInternal db);

  String getExecuteRule(@Nonnull DatabaseSessionInternal db);

  default String get(Scope scope, @Nonnull DatabaseSessionInternal db) {
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
