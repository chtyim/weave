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
import java.util.Map;

/**
*
*/
final class WeaveSpecificationCodec implements JsonSerializer<WeaveSpecification>,
                                               JsonDeserializer<WeaveSpecification> {

  @Override
  public JsonElement serialize(WeaveSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("name", src.getName());
    json.add("runnables", context.serialize(src.getRunnables(),
                                            new TypeToken<Map<String, RuntimeSpecification>>(){}.getType()));

    return json;
  }

  @Override
  public WeaveSpecification deserialize(JsonElement json, Type typeOfT,
                                        JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String name = jsonObj.get("name").getAsString();
    Map<String, RuntimeSpecification> runnables = context.deserialize(
      jsonObj.get("runnables"), new TypeToken<Map<String, RuntimeSpecification>>(){}.getType());

    return new DefaultWeaveSpecification(name, runnables);
  }
}
