package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
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

  String getName(@Nonnull DatabaseSessionInternal session);

  boolean isActive(@Nonnull DatabaseSessionInternal session);

  String getCreateRule(@Nonnull DatabaseSessionInternal session);

  String getReadRule(@Nonnull DatabaseSessionInternal session);

  String getBeforeUpdateRule(@Nonnull DatabaseSessionInternal session);

  String getAfterUpdateRule(@Nonnull DatabaseSessionInternal session);

  String getDeleteRule(@Nonnull DatabaseSessionInternal session);

  String getExecuteRule(@Nonnull DatabaseSessionInternal session);

  default String get(Scope scope, @Nonnull DatabaseSessionInternal session) {
    return switch (scope) {
      case CREATE -> getCreateRule(session);
      case READ -> getReadRule(session);
      case BEFORE_UPDATE -> getBeforeUpdateRule(session);
      case AFTER_UPDATE -> getAfterUpdateRule(session);
      case DELETE -> getDeleteRule(session);
      case EXECUTE -> getExecuteRule(session);
    };
  }
}
