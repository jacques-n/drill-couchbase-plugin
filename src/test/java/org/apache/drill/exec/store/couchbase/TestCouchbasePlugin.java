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

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestCouchbasePlugin extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCouchbasePlugin.class);

  @Test
  public void testListBuckets() throws Exception{
    test("use couchbase;");
    test("show tables;");
    test("select key, b.row_val.name, b.row_val.abv, b.row_val.type"
        + " from (select key, convert_from(row_value, 'JSON') as row_val from `beer-sample`) b"
        + " where b.row_val.abv > 5 limit 25;");
  }

}
