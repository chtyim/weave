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

import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.api.DefaultWeaveSpecification;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of gson serializer/deserializer {@link WeaveSpecification}.
 */
final class WeaveSpecificationCodec implements JsonSerializer<WeaveSpecification>,
                                               JsonDeserializer<WeaveSpecification> {

  @Override
  public JsonElement serialize(WeaveSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("name", src.getName());
    json.add("runnables", context.serialize(src.getRunnables(),
                                            new TypeToken<Map<String, RuntimeSpecification>>(){}.getType()));
    json.add("orders", context.serialize(src.getOrders(),
                                         new TypeToken<List<WeaveSpecification.Order>>(){}.getType()));

    return json;
  }

  @Override
  public WeaveSpecification deserialize(JsonElement json, Type typeOfT,
                                        JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String name = jsonObj.get("name").getAsString();
    Map<String, RuntimeSpecification> runnables = context.deserialize(
      jsonObj.get("runnables"), new TypeToken<Map<String, RuntimeSpecification>>(){}.getType());
    List<WeaveSpecification.Order> orders = context.deserialize(
      jsonObj.get("orders"), new TypeToken<List<WeaveSpecification.Order>>(){}.getType());

    return new DefaultWeaveSpecification(name, runnables, orders);
  }

  static final class WeaveSpecificationOrderCoder implements JsonSerializer<WeaveSpecification.Order>,
                                                             JsonDeserializer<WeaveSpecification.Order> {

    @Override
    public JsonElement serialize(WeaveSpecification.Order src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.add("names", context.serialize(src.getNames(), new TypeToken<Set<String>>(){}.getType()));
      json.addProperty("type", src.getType().name());
      return json;
    }

    @Override
    public WeaveSpecification.Order deserialize(JsonElement json, Type typeOfT,
                                                JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();

      Set<String> names = context.deserialize(jsonObj.get("names"), new TypeToken<Set<String>>(){}.getType());
      WeaveSpecification.Order.Type type = WeaveSpecification.Order.Type.valueOf(jsonObj.get("type").getAsString());

      return new DefaultWeaveSpecification.DefaultOrder(names, type);
    }
  }
}
