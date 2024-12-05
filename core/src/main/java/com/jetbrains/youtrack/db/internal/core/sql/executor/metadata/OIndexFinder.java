package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import java.util.Optional;

public interface OIndexFinder {

  enum Operation {
    Eq,
    Gt,
    Lt,
    Ge,
    Le,
    FuzzyEq,
    Range;

    public boolean isRange() {
      return this == Gt || this == Lt || this == Ge || this == Le;
    }
  }

  Optional<OIndexCandidate> findExactIndex(OPath fieldName, Object value, CommandContext ctx);

  Optional<OIndexCandidate> findByKeyIndex(OPath fieldName, Object value, CommandContext ctx);

  Optional<OIndexCandidate> findAllowRangeIndex(
      OPath fieldName, Operation operation, Object value, CommandContext ctx);

  Optional<OIndexCandidate> findByValueIndex(OPath fieldName, Object value, CommandContext ctx);

  Optional<OIndexCandidate> findFullTextIndex(OPath fieldName, Object value, CommandContext ctx);
}
