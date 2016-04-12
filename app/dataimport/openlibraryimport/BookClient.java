package dataimport.openlibraryimport;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by P. Akhmedzianov on 11.03.2016.
 */
public class BookClient {
    private static final String API_BASE_URL = "http://openlibrary.org/";
    private CloseableHttpAsyncClient httpСlient_;

    public BookClient() {
        this.httpСlient_ = HttpAsyncClients.createDefault();
        httpСlient_.start();
    }

    private String getApiUrl(final String relativeUrl) {
        return API_BASE_URL + relativeUrl;
    }

    public HashMap<String, JSONObject> getBooksByStringMap(final ArrayList<String> stringArrayList) {
        System.out.println("Got " + stringArrayList.size() + " ISBN's.");
        HashMap<String, JSONObject> res = new HashMap<>();
        stringArrayList.parallelStream().forEach(str -> {
            try {
                res.put(str, this.getBooksByIsbn(JsonParser.getIsbnFromString(str), this.httpСlient_));
            } catch (Exception e) {
                // got wrong ISBN
                e.printStackTrace();
                res.put(str, null);
            }
        });
        return res;
    }

    // Method for accessing the search API
    // written in a very dangerous manner to long live autonomous getting information and coz of openlibrary api issues
    public JSONObject getBooksByIsbn(final String isbn, final CloseableHttpAsyncClient httpClient) {
        final HttpGet httpGetRequest = new HttpGet(getApiUrl("search.json?isbn=") + isbn);
        try {
            HttpResponse response = null;
            //requesting service until getting positive ok 200 response
            while (response == null || response.getStatusLine().getStatusCode() != 200) {
                Future<HttpResponse> future = httpClient.execute(httpGetRequest, null);
                response = future.get();
            }
            String json_string = EntityUtils.toString(response.getEntity());
            return new JSONObject(json_string);
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Caught ExecutionException!!!");
            return getBooksByIsbn(isbn, httpClient);
        } catch (JSONException e) {
            System.out.println("Caught JSONException!!!");
            return getBooksByIsbn(isbn, httpClient);
        } catch (IOException e) {
            System.out.println("Caught IOException!!!");
            return getBooksByIsbn(isbn, httpClient);
        }
    }

    public void test() {

    }
}
