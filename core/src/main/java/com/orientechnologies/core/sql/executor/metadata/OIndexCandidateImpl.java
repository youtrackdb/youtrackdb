package com.orientechnologies.core.sql.executor.metadata;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class OIndexCandidateImpl implements OIndexCandidate {

  private final String name;
  private Operation operation;
  private final YTProperty property;

  public OIndexCandidateImpl(String name, Operation operation, YTProperty prop) {
    this.name = name;
    this.operation = operation;
    this.property = prop;
  }

  public String getName() {
    return name;
  }

  @Override
  public Optional<OIndexCandidate> invert() {
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
  public Optional<OIndexCandidate> normalize(OCommandContext ctx) {
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(name);
    if (property.getName().equals(index.getDefinition().getFields().get(0))) {
      return Optional.of(this);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public List<YTProperty> properties() {
    return Collections.singletonList(this.property);
  }
}
