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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

@JsonTypeName("couchbase-partition-scan")
public class CouchbaseSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchbaseSubScan.class);

  @JsonProperty
  public final CouchbaseStoragePluginConfig storage;
  @JsonIgnore
  private final CouchbaseStoragePlugin plugin;

  private String bucket;

  @JsonCreator
  public CouchbaseSubScan(@JacksonInject StoragePluginRegistry registry,
                      @JsonProperty("bucket") String bucket,
                      @JsonProperty("storage") StoragePluginConfig storage) throws ExecutionSetupException {
    plugin = (CouchbaseStoragePlugin) registry.getPlugin(storage);
    this.bucket = bucket;
    this.storage = (CouchbaseStoragePluginConfig) storage;
  }

  public CouchbaseSubScan(CouchbaseStoragePlugin plugin, CouchbaseStoragePluginConfig config,
      String bucket) {
    this.plugin = plugin;
    storage = config;
    this.bucket = bucket;
  }

  @JsonIgnore
  public CouchbaseStoragePluginConfig getStorageConfig() {
    return storage;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public CouchbaseStoragePlugin getStorageEngine(){
    return plugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new CouchbaseSubScan(plugin, storage, bucket);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public int getOperatorType() {
    return 2001;
  }

}
