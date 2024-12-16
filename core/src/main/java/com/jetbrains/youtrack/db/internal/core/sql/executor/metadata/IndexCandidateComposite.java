package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.IndexFinder.Operation;
import java.util.List;
import java.util.Optional;

public class IndexCandidateComposite implements IndexCandidate {

  private final String index;
  private final Operation operation;
  private final List<Property> properties;

  public IndexCandidateComposite(String index, Operation operation, List<Property> properties) {
    this.index = index;
    this.operation = operation;
    this.properties = properties;
  }

  @Override
  public String getName() {
    return index;
  }

  @Override
  public Optional<IndexCandidate> invert() {
    return Optional.empty();
  }

  @Override
  public Operation getOperation() {
    return operation;
  }

  @Override
  public Optional<IndexCandidate> normalize(CommandContext ctx) {
    return Optional.of(this);
  }

  @Override
  public List<Property> properties() {
    return properties;
  }
}
