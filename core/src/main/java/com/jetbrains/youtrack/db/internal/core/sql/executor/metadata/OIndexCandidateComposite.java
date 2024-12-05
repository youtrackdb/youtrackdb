package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.List;
import java.util.Optional;

public class OIndexCandidateComposite implements OIndexCandidate {

  private final String index;
  private final Operation operation;
  private final List<YTProperty> properties;

  public OIndexCandidateComposite(String index, Operation operation, List<YTProperty> properties) {
    this.index = index;
    this.operation = operation;
    this.properties = properties;
  }

  @Override
  public String getName() {
    return index;
  }

  @Override
  public Optional<OIndexCandidate> invert() {
    return Optional.empty();
  }

  @Override
  public Operation getOperation() {
    return operation;
  }

  @Override
  public Optional<OIndexCandidate> normalize(CommandContext ctx) {
    return Optional.of(this);
  }

  @Override
  public List<YTProperty> properties() {
    return properties;
  }
}
