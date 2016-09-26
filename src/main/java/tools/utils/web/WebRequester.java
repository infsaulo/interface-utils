package tools.utils.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.api.client.util.Charsets;

public class WebRequester {

  private static final Logger LOGGER = Logger.getLogger(WebRequester.class.getName());

  public static String doGet(final String urlLink) throws IOException {

    final URL url = new URL(urlLink);
    final URLConnection conn = url.openConnection();

    final String response = readFromInputStream(conn.getInputStream());

    return response;
  }

  public static String doPost(final String urlLink, final String payload,
      final Map<String, String> headers) throws IOException {

    final URL url = new URL(urlLink);
    final URLConnection con = url.openConnection();
    final HttpURLConnection http = (HttpURLConnection) con;

    final byte[] out = payload.getBytes(Charsets.UTF_8);
    final int length = out.length;

    http.setRequestMethod("POST");
    http.setDoOutput(true);
    http.setFixedLengthStreamingMode(length);
    http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

    if (headers != null) {
      final Iterator<Map.Entry<String, String>> it = headers.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry<String, String> pair = it.next();
        http.setRequestProperty(pair.getKey(), pair.getValue());
      }
    }
    http.connect();
    try (OutputStream os = http.getOutputStream()) {
      os.write(out);
    }

    final String response = readFromInputStream(http.getInputStream());

    return response;
  }

  private static String readFromInputStream(final InputStream input) {

    final BufferedReader br = new BufferedReader(new InputStreamReader(input, Charsets.UTF_8));

    final StringBuffer sb = new StringBuffer();

    String inputLine;
    try {
      while ((inputLine = br.readLine()) != null) {

        sb.append(inputLine.trim() + "\n");
      }
    } catch (final IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }

    return sb.toString();
  }
}
