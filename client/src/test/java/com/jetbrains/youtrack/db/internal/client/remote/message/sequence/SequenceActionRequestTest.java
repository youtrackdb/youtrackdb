/*
 * Copyright 2018 YouTrackDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.internal.client.remote.message.sequence;

import com.jetbrains.youtrack.db.internal.client.remote.message.sequence.SequenceActionRequest;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.Sequence;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.SequenceAction;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class SequenceActionRequestTest {

  @Test
  public void testSerializeDeserialize() {
    Sequence.CreateParams params = new Sequence.CreateParams().setLimitValue(123L);
    SequenceAction action =
        new SequenceAction(
            SequenceAction.CREATE, "testName", params, Sequence.SEQUENCE_TYPE.ORDERED);
    SequenceActionRequest request = new SequenceActionRequest(action);
    ByteArrayOutputStream arrayOutput = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(arrayOutput);
    try {
      request.serialize(out);
      arrayOutput.flush();
      byte[] bytes = arrayOutput.toByteArray();
      arrayOutput.close();

      ByteArrayInputStream arrayInput = new ByteArrayInputStream(bytes);
      DataInput in = new DataInputStream(arrayInput);
      SequenceActionRequest newRequest = new SequenceActionRequest();
      newRequest.deserialize(in);

      Assert.assertEquals(newRequest.getAction().getActionType(), action.getActionType());
      Assert.assertEquals(newRequest.getAction().getSequenceName(), action.getSequenceName());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getCacheSize(),
          action.getParameters().getCacheSize());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getIncrement(),
          action.getParameters().getIncrement());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getLimitValue(),
          action.getParameters().getLimitValue());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getOrderType(),
          action.getParameters().getOrderType());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getRecyclable(),
          action.getParameters().getRecyclable());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getStart(), action.getParameters().getStart());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getCurrentValue(),
          action.getParameters().getCurrentValue());
    } catch (IOException exc) {
      Assert.fail();
    }
  }
}
