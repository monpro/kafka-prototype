package com.github.kafka.stream;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    public static void main(String[] args) {
        //create properties

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_topics");

        KStream<String, String> filteredStream = inputTopic.filter(
                (k, tweet) -> getUserFollowersCount(tweet) > 10000
        );

        //
    }
    private static JsonParser jsonParser = new JsonParser();

    private static Integer getUserFollowersCount(String tweet){
        try {
            return jsonParser.parse(tweet).
                    getAsJsonObject().
                    get("user").
                    getAsJsonObject().
                    get("followers_count").
                    getAsInt();
        }catch (NullPointerException e){
            return 0;
        }
    }
}
