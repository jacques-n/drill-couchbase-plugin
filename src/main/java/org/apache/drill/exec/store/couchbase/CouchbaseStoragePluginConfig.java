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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.drill.common.logical.StoragePluginConfigBase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;

@JsonTypeName(CouchbaseStoragePluginConfig.NAME)
public class CouchbaseStoragePluginConfig extends StoragePluginConfigBase implements DrillCouchbaseConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchbaseStoragePluginConfig.class);

  private List<String> uris;
  private String username;
  private String password;

  public static final String NAME = "couchbase";

  @JsonCreator
  public CouchbaseStoragePluginConfig(@JsonProperty("uris") List<String> uris, @JsonProperty("login") String username,
      @JsonProperty("password") String password) {
    this.uris = uris;
    this.username = username;
    this.password = password;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((password == null) ? 0 : password.hashCode());
    result = prime * result + ((uris == null) ? 0 : uris.hashCode());
    result = prime * result + ((username == null) ? 0 : username.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CouchbaseStoragePluginConfig other = (CouchbaseStoragePluginConfig) obj;
    if (password == null) {
      if (other.password != null)
        return false;
    } else if (!password.equals(other.password))
      return false;
    if (uris == null) {
      if (other.uris != null)
        return false;
    } else if (!uris.equals(other.uris))
      return false;
    if (username == null) {
      if (other.username != null)
        return false;
    } else if (!username.equals(other.username))
      return false;
    return true;
  }

  @JsonIgnore 
  List<URI> getUrisAsURIs() throws URISyntaxException{
    List<URI> u = Lists.newArrayListWithCapacity(this.uris.size());
    for(String str : uris){
      u.add((new URI(str)));
    }
    return u;
  }

  public List<String> getUris() {
    return uris;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

}
