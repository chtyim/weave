package com.continuuity.weave.internal.state;

import com.continuuity.weave.api.Command;
import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public class MessageCodecTest {

  @Test
  public void testCodec() {
    Message message = Messages.decode(Messages.encode(new Message() {

      @Override
      public String getId() {
        return "message-id";
      }

      @Override
      public Scope getScope() {
        return Scope.APPLICATION;
      }

      @Override
      public String getRunnableName() {
        return null;
      }

      @Override
      public Command getCommand() {
        return new Command() {
          @Override
          public String getCommand() {
            return "stop";
          }

          @Override
          public Map<String, String> getOptions() {
            return ImmutableMap.of("timeout", "1", "timeoutUnit", "SECONDS");
          }
        };
      }
    }));

    Assert.assertEquals("message-id", message.getId());
    Assert.assertEquals(Message.Scope.APPLICATION, message.getScope());
    Assert.assertNull(message.getRunnableName());
    Assert.assertEquals("stop", message.getCommand().getCommand());
    Assert.assertEquals(ImmutableMap.of("timeout", "1", "timeoutUnit", "SECONDS"), message.getCommand().getOptions());
  }

  @Test
  public void testFailureDecode() {
    Assert.assertNull(Messages.decode("".getBytes()));
  }
}
