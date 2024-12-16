package com.jetbrains.youtrack.db.internal.core.tx;

public enum ValidationResult {
  VALID,
  ALREADY_PROMISED,
  MISSING_PREVIOUS,
  ALREADY_PRESENT,
}
