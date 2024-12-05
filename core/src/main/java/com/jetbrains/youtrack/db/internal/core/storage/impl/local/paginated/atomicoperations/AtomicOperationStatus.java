package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations;

public enum AtomicOperationStatus {
  NOT_STARTED,
  IN_PROGRESS,
  COMMITTED,
  ROLLED_BACK,
  PERSISTED
}
