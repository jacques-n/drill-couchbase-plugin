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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;

import com.google.common.base.Stopwatch;

public class CouchbaseRecordReader extends AbstractRecordReader implements DrillCouchbaseConstants {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchbaseRecordReader.class);

  private static final int TARGET_RECORD_COUNT = 4000;

  private OutputMutator outputMutator;
  private FragmentContext fragmentContext;
  private OperatorContext operatorContext;


  public CouchbaseRecordReader(CouchbaseSubScan subScanSpec,
      List<SchemaPath> projectedColumns, FragmentContext context) throws OutOfMemoryException {
    fragmentContext = context;
    setColumns(projectedColumns);
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
  }

  @Override
  public int next() {
    Stopwatch watch = new Stopwatch();
    watch.start();

    int rowCount = 0;
    for (; rowCount < TARGET_RECORD_COUNT; rowCount++) {
      break;
    }

    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), rowCount);
    return rowCount;
  }

  @Override
  public void cleanup() {
  }

}
