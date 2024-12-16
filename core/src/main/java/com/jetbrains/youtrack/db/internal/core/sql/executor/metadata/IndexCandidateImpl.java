package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.IndexFinder.Operation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class IndexCandidateImpl implements IndexCandidate {

  private final String name;
  private Operation operation;
  private final Property property;

  public IndexCandidateImpl(String name, Operation operation, Property prop) {
    this.name = name;
    this.operation = operation;
    this.property = prop;
  }

  public String getName() {
    return name;
  }

  @Override
  public Optional<IndexCandidate> invert() {
    if (this.operation == Operation.Ge) {
      this.operation = Operation.Lt;
    } else if (this.operation == Operation.Gt) {
      this.operation = Operation.Le;
    } else if (this.operation == Operation.Le) {
      this.operation = Operation.Gt;
    } else if (this.operation == Operation.Lt) {
      this.operation = Operation.Ge;
    }
    return Optional.of(this);
  }

  public Operation getOperation() {
    return operation;
  }

  @Override
  public Optional<IndexCandidate> normalize(CommandContext ctx) {
    Index index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(name);
    if (property.getName().equals(index.getDefinition().getFields().get(0))) {
      return Optional.of(this);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public List<Property> properties() {
    return Collections.singletonList(this.property);
  }
}
