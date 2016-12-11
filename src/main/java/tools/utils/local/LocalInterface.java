package tools.utils.local;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LocalInterface {

  private static final Logger LOGGER = Logger.getLogger(LocalInterface.class.getName());

  private Writer writer;

  public LocalInterface(final String filePath, final boolean append) throws IOException {

    try {

      writer = new OutputStreamWriter(new FileOutputStream(filePath, append), StandardCharsets
          .UTF_8);
    } catch (IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }

  public void writeToFile(String content) {

    synchronized (writer) {
      try {

        writer.write(content);
        writer.flush();
      } catch (IOException e) {

        LOGGER.log(Level.SEVERE, e.toString(), e);
      }
    }
  }

  public void closeInterface() {

    try {

      writer.close();
    } catch (IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }
  }
}
