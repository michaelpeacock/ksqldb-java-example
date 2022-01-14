package io.confuent.ksql;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.TopicInfo;

import static java.util.stream.IntStream.range;

public class Application {
  public static String KSQLDB_SERVER_HOST = "localhost";
  public static int KSQLDB_SERVER_HOST_PORT = 8088;

  public static void main(String[] args) throws ExecutionException, InterruptedException, MalformedURLException {

    final ClientOptions clientOptions = ClientOptions.create()
        .setHost(KSQLDB_SERVER_HOST)
        .setPort(KSQLDB_SERVER_HOST_PORT);
    final Client client = Client.create(clientOptions);

    // print all topics
    final List<TopicInfo> topicInfos = client.listTopics().get();
    topicInfos.forEach(System.out::println);
    final Map<String, Object> properties = Map.of("auto.offset.reset", "earliest");

    // listing all tables
    client.listTables().get().forEach(System.out::println);

    // listing all streams
    client.listStreams().get().forEach(System.out::println);

    final StreamedQueryResult
        streamedQueryResult =
        client.streamQuery("select * from SQUIRE_STATUS EMIT CHANGES;", properties).get();

    range(0, 1000)
        .mapToObj(i -> streamedQueryResult.poll()).forEach(row -> {
          if (row != null) {
            System.out.println("Received a row!");
            System.out.println("Row: " + row.values());
          } else {
            System.out.println("Query has ended.");
          }
        });
    client.close();
  }
}