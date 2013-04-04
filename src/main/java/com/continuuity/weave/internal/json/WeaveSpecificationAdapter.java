package com.continuuity.weave.internal.json;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 */
public final class WeaveSpecificationAdapter {

  private final Gson gson;

  public static WeaveSpecificationAdapter create() {
    return new WeaveSpecificationAdapter();
  }

  private WeaveSpecificationAdapter() {
    gson = new GsonBuilder()
              .serializeNulls()
              .registerTypeAdapter(WeaveSpecification.class, new WeaveSpecificationCodec())
              .registerTypeAdapter(RuntimeSpecification.class, new RuntimeSpecificationCodec())
              .registerTypeAdapter(WeaveRunnableSpecification.class, new WeaveRunnableSpecificationCodec())
              .registerTypeAdapter(ResourceSpecification.class, new ResourceSpecificationCodec())
              .registerTypeAdapter(LocalFile.class, new LocalFileCodec())
              .registerTypeAdapterFactory(new WeaveSpecificationTypeAdapterFactory())
              .create();
  }

  public String toJson(WeaveSpecification spec) {
    return gson.toJson(spec, WeaveSpecification.class);
  }

  public void toJson(WeaveSpecification spec, Writer writer) {
    gson.toJson(spec, WeaveSpecification.class, writer);
  }

  public WeaveSpecification fromJson(String json) {
    return gson.fromJson(json, WeaveSpecification.class);
  }

  public WeaveSpecification fromJson(Reader reader) {
    return gson.fromJson(reader, WeaveSpecification.class);
  }

  // This is to get around gson ignoring of inner class
  private static final class WeaveSpecificationTypeAdapterFactory implements TypeAdapterFactory {

    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
      Class<?> rawType = type.getRawType();
      if (!Map.class.isAssignableFrom(rawType)) {
        return null;
      }
      Type[] typeArgs = ((ParameterizedType) type.getType()).getActualTypeArguments();
      TypeToken<?> keyType = TypeToken.get(typeArgs[0]);
      TypeToken<?> valueType = TypeToken.get(typeArgs[1]);
      if (keyType.getRawType() != String.class) {
        return null;
      }
      return (TypeAdapter<T>) mapAdapter(gson, valueType);
    }

    private <V> TypeAdapter<Map<String, V>> mapAdapter(Gson gson, TypeToken<V> valueType) {
      final TypeAdapter<V> valueAdapter = gson.getAdapter(valueType);

      return new TypeAdapter<Map<String, V>>() {
        @Override
        public void write(JsonWriter writer, Map<String, V> map) throws IOException {
          if (map == null) {
            writer.nullValue();
            return;
          }
          writer.beginObject();
          for (Map.Entry<String, V> entry : map.entrySet()) {
            writer.name(entry.getKey());
            valueAdapter.write(writer, entry.getValue());
          }
          writer.endObject();
        }

        @Override
        public Map<String, V> read(JsonReader reader) throws IOException {
          if (reader.peek() == JsonToken.NULL) {
            reader.nextNull();
            return null;
          }
          if (reader.peek() != JsonToken.BEGIN_OBJECT) {
            return null;
          }
          Map<String, V> map = Maps.newHashMap();
          reader.beginObject();
          while (reader.peek() != JsonToken.END_OBJECT) {
            map.put(reader.nextName(), valueAdapter.read(reader));
          }
          reader.endObject();
          return map;
        }
      };
    }
  }
}
