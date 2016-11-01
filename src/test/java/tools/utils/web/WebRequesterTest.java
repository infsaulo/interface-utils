package tools.utils.web;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.wizzardo.tools.json.JsonTools;

public class WebRequesterTest {

  @Test
  public void testGetUrl() {

    try {

      final String pageContent = WebRequester.doGet("https://www.google.com", null, false);
      Assert.assertNotNull(pageContent);
    } catch (final IOException e) {

      e.printStackTrace();
    }

  }

  @Test
  public void testPostUrl() {

    try {

      final Map<String, Long> data = new HashMap<>();
      data.put("field", 1L);
      data.put("field2", 2L);
      final String postContent =
          WebRequester.doPost("http://httpbin.org/post", JsonTools.serialize(data), null, false);
      Assert.assertNotNull(postContent);
    } catch (final IOException e) {

      e.printStackTrace();
    }

  }

  @Test
  public void testHeaders() {

    final Map<String, String> headers = new HashMap<>();
    headers.put("Header1", "value 1");
    headers.put("Header2", "value 2");
    try {

      final String headersContent =
          WebRequester.doPost("http://httpbin.org/post", "", headers, false);
      Assert.assertNotNull(headersContent);
    } catch (final IOException e) {

      e.printStackTrace();
    }
  }

}
