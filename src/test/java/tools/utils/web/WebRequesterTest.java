package tools.utils.web;

import com.wizzardo.tools.json.JsonTools;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        final Map<String, Object> headers = new HashMap<>();
        final List<String> header1 = new ArrayList<>();
        header1.add("value 1");
        final List<String> header2 = new ArrayList<>();
        header2.add("value 2");
        headers.put("Header1", header1);
        headers.put("Header2", header2);
        try {

            final String headersContent =
                    WebRequester.doPost("http://httpbin.org/post", "", headers, false);
            Assert.assertNotNull(headersContent);
        } catch (final IOException e) {

            e.printStackTrace();
        }
    }

}
