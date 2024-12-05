package com.orientechnologies.core.tx;

public enum ValidationResult {
  VALID,
  ALREADY_PROMISED,
  MISSING_PREVIOUS,
  ALREADY_PRESENT,
}
