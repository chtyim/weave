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
