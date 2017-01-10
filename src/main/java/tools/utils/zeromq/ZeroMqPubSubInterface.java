package tools.utils.zeromq;

import com.google.api.client.util.Charsets;
import com.google.api.client.util.Preconditions;

import org.zeromq.ZMQ;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ZeroMqPubSubInterface {

  private static final Logger LOGGER = Logger.getLogger(ZeroMqPubSubInterface.class.getName());
  final ZMQ.Context context;
  final ZMQ.Socket socket;

  public ZeroMqPubSubInterface(final String url, final int socketMode) {

    Preconditions.checkArgument(socketMode == ZMQ.PUB || socketMode == ZMQ.SUB);

    context = ZMQ.context(1);
    socket = context.socket(socketMode);

    if (socketMode == ZMQ.PUB) {

      socket.bind(url);

      LOGGER.log(Level.INFO, "Publisher listening to " + url);
    } else {

      LOGGER.log(Level.INFO, "Subscriber connected to " + url);
      socket.connect(url);
    }
  }

  protected void finalize() throws Throwable {

    socket.close();
    context.term();

    super.finalize();
  }

  public void subscribe(final byte[] topicSub) {

    Preconditions.checkArgument(topicSub != null);

    socket.subscribe(topicSub);

    LOGGER.log(Level.INFO, "subscribing to " + new String(topicSub, Charsets.UTF_8));
  }

  public void unsubscribe(final byte[] topicSub) {

    Preconditions.checkArgument(topicSub != null);

    socket.unsubscribe(topicSub);

    LOGGER.log(Level.INFO, "unsubscribing to " + new String(topicSub, Charsets.UTF_8));
  }

  public byte[] readMsg() {

    Preconditions.checkState(socket.getType() == ZMQ.SUB);

    return socket.recv();
  }

  public void sendMsg(final byte[] msg) {

    Preconditions.checkState(socket.getType() == ZMQ.PUB);

    socket.send(msg);
  }
}
