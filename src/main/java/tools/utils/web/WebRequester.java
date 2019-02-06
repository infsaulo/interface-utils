package tools.utils.web;

import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.Charsets;
import com.google.api.client.util.ExponentialBackOff;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebRequester {

    private static final Logger LOGGER = Logger.getLogger(WebRequester.class.getName());

    public static String doGet(final String urlLink, final Map<String, ? extends Object> headers,
                               final boolean trimFirstLine) throws IOException {

        final URL url = new URL(urlLink);

        final HttpTransport transport = new NetHttpTransport();
        final HttpRequestFactory factory = transport.createRequestFactory();
        final GenericUrl urlGeneric = new GenericUrl(url);
        final HttpRequest request = factory.buildGetRequest(urlGeneric);

        final ExponentialBackOff backOff =
                new ExponentialBackOff.Builder().setInitialIntervalMillis(2000).setMaxIntervalMillis(60000)
                        .setMaxElapsedTimeMillis(600000).setMultiplier(1.5).setRandomizationFactor(0.5).build();
        request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backOff));
        request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(backOff));


        if (headers != null) {
            final HttpHeaders headersHttp = new HttpHeaders();

            final Iterator it = headers.entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<String, Object> pair = (Map.Entry<String, Object>)it.next();
                if (pair.getKey().equals("Authorization")) {

                    headersHttp.setAuthorization(pair.getValue().toString());
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
                                final Map<String, ? extends Object> headers, final boolean trimFirstLine) throws IOException {

        final URL url = new URL(urlLink);

        final HttpTransport transport = new NetHttpTransport();
        final HttpRequestFactory factory = transport.createRequestFactory();
        final GenericUrl urlGeneric = new GenericUrl(url);

        final HttpRequest request = factory.buildPostRequest(urlGeneric, ByteArrayContent.fromString(null, payload));
        final ExponentialBackOff backOff =
                new ExponentialBackOff.Builder().setInitialIntervalMillis(2000).setMaxIntervalMillis(60000)
                        .setMaxElapsedTimeMillis(600000).setMultiplier(1.5).setRandomizationFactor(0.5).build();
        request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backOff));
        request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(backOff));

        if (headers != null) {
            final Iterator it = headers.entrySet().iterator();
            final HttpHeaders httpHeaders = new HttpHeaders();
            while (it.hasNext()) {
                final Map.Entry<String, Object> pair = (Map.Entry<String, Object>) it.next();
                httpHeaders.set(pair.getKey(), pair.getValue());
            }
            request.setHeaders(httpHeaders);
        }

        final HttpResponse responseHttp = request.execute();


        final String response = readFromInputStream(responseHttp.getContent(), trimFirstLine);

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
