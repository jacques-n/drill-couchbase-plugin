/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.couchbase;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapStream;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;

import com.couchbase.client.TapClient;
import com.google.common.base.Stopwatch;

public class CouchbaseRecordReader extends AbstractRecordReader implements DrillCouchbaseConstants {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchbaseRecordReader.class);

  private static final int TARGET_RECORD_COUNT = 4000;

  private OutputMutator outputMutator;

  VarCharVector keyVector;
  VarBinaryVector valueVector;

  private ResponseMessage leftOver;

  private OperatorContext operatorContext;

  private TapClient tapClient;

  private List<URI> uris;

  private String bucket;

  public CouchbaseRecordReader(FragmentContext context, List<URI> uris, String bucket) {
    this.uris = uris;
    this.bucket = bucket;
  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }


  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    tapClient = new TapClient(uris, bucket, "");
    try {
      @SuppressWarnings("unused")
      TapStream testing = tapClient.tapDump("testing");
      MaterializedField keyField = MaterializedField.create("key", Types.required(MinorType.VARCHAR));
      keyVector = outputMutator.addField(keyField, VarCharVector.class);
      MaterializedField valueField = MaterializedField.create("row_value", Types.required(MinorType.VARBINARY));
      valueVector = outputMutator.addField(valueField, VarBinaryVector.class);
    } catch (SchemaChangeException | ConfigurationException | IOException  e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = new Stopwatch();
    watch.start();

    keyVector.clear();
    keyVector.allocateNew();
    valueVector.clear();
    valueVector.allocateNew();
    int rowCount = 0;
    done:
    for (; rowCount < TARGET_RECORD_COUNT && tapClient.hasMoreMessages();) {
      ResponseMessage message = null;
      if (leftOver != null) {
        message = leftOver;
        leftOver = null;
      } else {
        if ((message = tapClient.getNextMessage()) == null) {
          continue;
        }
      }

      if (!keyVector.getMutator().setSafe(rowCount, message.getKey().getBytes())) {
        setOutputRowCount(rowCount);
        leftOver = message;
        break done;
      }

      if (!valueVector.getMutator().setSafe(rowCount, message.getValue())) {
        setOutputRowCount(rowCount);
        leftOver = message;
        break done;
      }

      rowCount++;
    }

    setOutputRowCount(rowCount);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), rowCount);
    return rowCount;
  }


  @Override
  public void cleanup() {
    tapClient.shutdown();
  }

  private void setOutputRowCount(int count) {
    keyVector.getMutator().setValueCount(count);
    valueVector.getMutator().setValueCount(count);
  }

}
