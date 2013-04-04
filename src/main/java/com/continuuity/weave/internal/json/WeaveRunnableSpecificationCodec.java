package com.continuuity.weave.internal.json;

import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.internal.api.DefaultWeaveRunnableSpecification;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 */
final class WeaveRunnableSpecificationCodec implements JsonSerializer<WeaveRunnableSpecification>,
                                                       JsonDeserializer<WeaveRunnableSpecification> {

  @Override
  public JsonElement serialize(WeaveRunnableSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();

    json.addProperty("classname", src.getClassName());
    json.addProperty("name", src.getName());
    json.add("arguments", context.serialize(src.getArguments(), new TypeToken<Map<String, String>>(){}.getType()));

    return json;
  }

  @Override
  public WeaveRunnableSpecification deserialize(JsonElement json, Type typeOfT,
                                                JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("classname").getAsString();
    String name = jsonObj.get("name").getAsString();
    Map<String, String> arguments = context.deserialize(jsonObj.get("arguments"),
                                                        new TypeToken<Map<String, String>>(){}.getType());

    return new DefaultWeaveRunnableSpecification(className, name, arguments);
  }
}
