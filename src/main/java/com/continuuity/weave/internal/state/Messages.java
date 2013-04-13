package com.continuuity.weave.internal.state;

import com.continuuity.weave.api.Command;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.GsonBuilder;
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
final class Messages {

  private static final Type OPTIONS_TYPE = new TypeToken<Map<String, String>>() {}.getType();

  /**
   * Returns a {@link Message} decoded from the given byte array, or null if it is unrecognized.
   * @param bytes
   * @return
   */
  static Message decode(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    String content = new String(bytes, Charsets.UTF_8);
    return new GsonBuilder().registerTypeAdapter(Message.class, new MessageCodec())
                            .create()
                            .fromJson(content, Message.class);
  }


  static byte[] encode(Message message) {
    return new GsonBuilder().registerTypeAdapter(Message.class, new MessageCodec())
                            .create()
                            .toJson(message, Message.class)
                            .getBytes(Charsets.UTF_8);
  }


  private static final class MessageCodec implements JsonSerializer<Message>, JsonDeserializer<Message> {

    @Override
    public Message deserialize(JsonElement json, Type typeOfT,
                               JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      final String id = jsonObj.get("id").getAsString();
      final Message.Scope scope = Message.Scope.valueOf(jsonObj.get("scope").getAsString());
      JsonElement name = jsonObj.get("runnableName");
      final String runnableName = (name == null || name.isJsonNull()) ? null :name.getAsString();

        JsonObject commandObj = jsonObj.get("command").getAsJsonObject();
      final String commandName = commandObj.get("command").getAsString();
      final Map<String, String> options = ImmutableMap.copyOf(
                          context.<Map<String, String>>deserialize(commandObj.get("options"), OPTIONS_TYPE));
      final Command command = new Command() {
        @Override
        public String getCommand() {
          return commandName;
        }

        @Override
        public Map<String, String> getOptions() {
          return options;
        }
      };
      return new Message() {
        @Override
        public String getId() {
          return id;
        }

        @Override
        public Scope getScope() {
          return scope;
        }

        @Override
        public String getRunnableName() {
          return runnableName;
        }

        @Override
        public Command getCommand() {
          return command;
        }
      };
    }

    @Override
    public JsonElement serialize(Message message, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObj = new JsonObject();
      jsonObj.addProperty("id", message.getId());
      jsonObj.addProperty("scope", message.getScope().name());
      jsonObj.addProperty("runnableName", message.getRunnableName());

      Command command = message.getCommand();
      JsonObject commandObj = new JsonObject();
      commandObj.addProperty("command", command.getCommand());
      commandObj.add("options", context.serialize(command.getOptions(), OPTIONS_TYPE));

      jsonObj.add("command", commandObj);

      return jsonObj;
    }
  }

  private Messages() {
  }
}
