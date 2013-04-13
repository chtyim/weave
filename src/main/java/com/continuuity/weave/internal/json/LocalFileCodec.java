/**
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.weave.internal.json;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.internal.api.DefaultLocalFile;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.net.URI;

/**
 *
 */
public final class LocalFileCodec implements JsonSerializer<LocalFile>, JsonDeserializer<LocalFile> {

  @Override
  public JsonElement serialize(LocalFile src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();

    json.addProperty("name", src.getName());
    json.addProperty("uri", src.getURI().toASCIIString());
    json.addProperty("archive", src.isArchive());
    json.addProperty("pattern", src.getPattern());

    return json;
  }

  @Override
  public LocalFile deserialize(JsonElement json, Type typeOfT,
                               JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String name = jsonObj.get("name").getAsString();
    URI uri = URI.create(jsonObj.get("uri").getAsString());
    boolean archive = jsonObj.get("archive").getAsBoolean();
    JsonElement pattern = jsonObj.get("pattern");

    return new DefaultLocalFile(name, uri, archive, pattern.isJsonNull() ? null : pattern.getAsString());
  }
}
