package com.continuuity.weave.internal.json;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.internal.api.DefaultRuntimeSpecification;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Collection;

/**
 *
 */
final class RuntimeSpecificationCodec implements JsonSerializer<RuntimeSpecification>,
                                                 JsonDeserializer<RuntimeSpecification> {

  @Override
  public JsonElement serialize(RuntimeSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("name", src.getName());
    json.add("runnable", context.serialize(src.getRunnableSpecification(), WeaveRunnableSpecification.class));
    json.add("resources", context.serialize(src.getResourceSpecification(), ResourceSpecification.class));
    json.add("files", context.serialize(src.getLocalFiles(), new TypeToken<Collection<LocalFile>>(){}.getType()));

    return json;
  }

  @Override
  public RuntimeSpecification deserialize(JsonElement json, Type typeOfT,
                                          JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String name = jsonObj.get("name").getAsString();
    WeaveRunnableSpecification runnable = context.deserialize(jsonObj.get("runnable"),
                                                               WeaveRunnableSpecification.class);
    ResourceSpecification resources = context.deserialize(jsonObj.get("resources"),
                                                          ResourceSpecification.class);
    Collection<LocalFile> files = context.deserialize(jsonObj.get("files"),
                                                      new TypeToken<Collection<LocalFile>>(){}.getType());

    return new DefaultRuntimeSpecification(name, runnable, resources, files);
  }
}