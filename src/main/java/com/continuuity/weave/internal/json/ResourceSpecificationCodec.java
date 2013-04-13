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

import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.internal.api.DefaultResourceSpecification;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 *
 */
final class ResourceSpecificationCodec implements JsonSerializer<ResourceSpecification>,
                                                  JsonDeserializer<ResourceSpecification> {

  @Override
  public JsonElement serialize(ResourceSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();

    json.addProperty("cores", src.getCores());
    json.addProperty("memorySize", src.getMemorySize());
    json.addProperty("uplink", src.getUplink());
    json.addProperty("downlink", src.getDownlink());

    return json;
  }

  @Override
  public ResourceSpecification deserialize(JsonElement json, Type typeOfT,
                                           JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    return new DefaultResourceSpecification(jsonObj.get("cores").getAsInt(),
                                            jsonObj.get("memorySize").getAsInt(),
                                            jsonObj.get("uplink").getAsInt(),
                                            jsonObj.get("downlink").getAsInt());
  }
}
