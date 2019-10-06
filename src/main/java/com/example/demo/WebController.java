package com.example.demo;

import com.google.cloud.spanner.*;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.DRIVER_NAME;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;


@Controller
@ResponseBody
public class WebController {

    private static final Log LOGGER = LogFactory.getLog(WebController.class);

    private final static String QUERY
        = "SELECT * FROM met_objects WHERE object_id < ";

    @Value("${gcp.project}")
    private String gcpProject;

    @Value("${spanner.instance}")
    private String spannerInstance;

    @Value("${spanner.database}")
    private String spannerDatabase;


    @GetMapping(value = "/art/reactive/{numRows}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getArtReactive(@PathVariable int numRows) {

        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(Option.valueOf("project"), gcpProject)
                .option(DRIVER, DRIVER_NAME)
                .option(INSTANCE, spannerInstance)
                .option(DATABASE, spannerDatabase)
                .build());

        long startTime = System.currentTimeMillis();

        return Mono.from(connectionFactory.create())
                .flatMapMany(connection -> connection
                        .createStatement(QUERY + numRows).execute())
                .flatMap(spannerResult -> spannerResult.map(
                        (r, meta) -> r.get("title", String.class)
                )).doOnComplete(
                        () -> LOGGER.info("Finished reactive: " + (System.currentTimeMillis() - startTime))
                );

    }

    @GetMapping(value = "/art/imperative/{numRows}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getArtBlocking(@PathVariable int numRows) {

        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();

        // Creates a database client
        DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
                options.getProjectId(), spannerInstance, spannerDatabase));

        long startTime = System.currentTimeMillis();

        // Queries the database
        ResultSet resultSet = dbClient.singleUse().executeQuery(Statement.of(QUERY + numRows));

        return Flux.<String>generate(sink -> {
            if (resultSet.next()) {
                sink.next(resultSet.getCurrentRowAsStruct().getString("title"));
            } else {
                sink.complete();
            }
        }).doOnComplete(
                () -> LOGGER.info("Finished imperative: " + (System.currentTimeMillis() - startTime))
        );

    }
}
