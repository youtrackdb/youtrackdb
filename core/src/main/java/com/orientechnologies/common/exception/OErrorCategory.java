package com.orientechnologies.common.exception;

/**
 *
 */
public enum OErrorCategory {
  GENERIC(1),

  SQL_GENERIC(2),

  SQL_PARSING(3),

  STORAGE(4),

  CONCURRENCY_RETRY(5),

  VALIDATION(6),

  CONCURRENCY(7);

  final int code;

  OErrorCategory(int code) {
    this.code = code;
  }
}
