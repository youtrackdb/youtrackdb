package com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy;

import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;

/** Raised when one traversal carries contradictory order-semantics instructions. */
public final class ContradictoryOrderSemanticsException extends CommandExecutionException {

  public ContradictoryOrderSemanticsException(String message) {
    super(message);
  }
}
