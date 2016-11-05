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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.Charsets;
import com.google.api.client.util.ExponentialBackOff;

public class WebRequester {

  private static final Logger LOGGER = Logger.getLogger(WebRequester.class.getName());

  public static String doGet(final String urlLink, final Map<String, String> headers,
      final boolean trimFirstLine) throws IOException {

    final URL url = new URL(urlLink);

    final HttpTransport transport = new NetHttpTransport();
    final HttpRequestFactory factory = transport.createRequestFactory();
    final GenericUrl urlGeneric = new GenericUrl(url);
    final HttpRequest request = factory.buildGetRequest(urlGeneric);

    final ExponentialBackOff backOff = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(120000).setMaxIntervalMillis(600000)
        .setMaxElapsedTimeMillis(900000).setMultiplier(1.5).setRandomizationFactor(0.5).build();
    request.setUnsuccessfulResponseHandler(
        new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()));

    if (headers != null) {
      final HttpHeaders headersHttp = new HttpHeaders();

      final Iterator<Map.Entry<String, String>> it = headers.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry<String, String> pair = it.next();
        if (pair.getKey().equals("Authorization")) {

          headersHttp.setAuthorization(pair.getValue());
        } else {

          headersHttp.set(pair.getKey(), pair.getValue());
        }
      }
      request.setHeaders(headersHttp);
    }

    final HttpResponse responseHttp = request.execute();

    final String response = readFromInputStream(responseHttp.getContent(), trimFirstLine);

    return response;
  }

  public static String doPost(final String urlLink, final String payload,
      final Map<String, String> headers, final boolean trimFirstLine) throws IOException {

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

    final String response = readFromInputStream(http.getInputStream(), trimFirstLine);

    return response;
  }

  private static String readFromInputStream(final InputStream input, final boolean trimFirstLine) {

    final BufferedReader br = new BufferedReader(new InputStreamReader(input, Charsets.UTF_8));

    final StringBuffer sb = new StringBuffer();

    String inputLine;

    try {

      if (trimFirstLine) {

        inputLine = br.readLine();
      }

      while ((inputLine = br.readLine()) != null) {

        sb.append(inputLine.trim() + "\n");
      }
    } catch (final IOException e) {

      LOGGER.log(Level.SEVERE, e.toString(), e);
    }

    return sb.toString();
  }
}
