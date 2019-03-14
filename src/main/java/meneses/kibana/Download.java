package meneses.kibana;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class Download {
  public static void main(String[] args) throws IOException {
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    String password = args[0];
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("sonarlint_telemetry", password));

    RestClient.FailureListener failureListener = new RestClient.FailureListener() {
      @Override
      public void onFailure(Node node) {
        System.out.println("FAILURE" + node);
      }
    };

    RestClientBuilder restClient = RestClient.builder(new HttpHost("f3229ee9ff02175207a44fd63499d622.us-east-1.aws.found.io", 9243, "https"))
      .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
      .setFailureListener(failureListener)
      .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(30000).setSocketTimeout(300000))
      .setMaxRetryTimeoutMillis(300000);

    try (RestHighLevelClient client = new RestHighLevelClient(restClient)) {
      Scroll scroll = new Scroll(TimeValue.timeValueHours(10L));
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.query(matchAllQuery());
      searchSourceBuilder.sort("timestamp");
      searchSourceBuilder.size(10000);
      searchSourceBuilder.timeout(TimeValue.timeValueMinutes(10));

      SearchRequest searchRequest = new SearchRequest("inactive-sonarlint-telemetry-2019-*");
      searchRequest.source(searchSourceBuilder);
      searchRequest.scroll(scroll);

      SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      String scrollId = searchResponse.getScrollId();
      SearchHit[] searchHits = searchResponse.getHits().getHits();

      Save save = new Save();
      save.before();

      while (searchHits != null && searchHits.length > 0) {
        for (int i = 0; i < searchHits.length; i++) {
          SearchHit hit = searchHits[i];
          save.processLine(hit);
        }

        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(scroll);
        searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
        scrollId = searchResponse.getScrollId();
        searchHits = searchResponse.getHits().getHits();
      }

      ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
      clearScrollRequest.addScrollId(scrollId);
      ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
      boolean succeeded = clearScrollResponse.isSucceeded();

      save.after();
    }
  }

}
