package com.orientechnologies.orient.core.metadata.security.jwt;

import java.io.DataInputStream;
import java.io.IOException;

public interface OTokenPayloadDeserializer {

  OBinaryTokenPayload deserialize(DataInputStream input, OTokenMetaInfo base) throws IOException;
}
