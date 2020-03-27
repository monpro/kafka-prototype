package twitter.stream;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey = "JIQ2EM3ObxKoZzBECKdVKvZLN";
    String consumerSecret = "30fxYp7ogCSpLN7hUTLQwNAYOlXAM4WQmsnEjGov0kkNcFczQg";
    String token = "1064278418798272512-YdOLOQjUAianiHu4OFmDkzbBA2nVEp";
    String secret = "BLKmnLKbCbRpGsSXNIjF5zGlXQrr9209MI06yOfhbGmiX";

    private TwitterProducer(){

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run(){
        logger.info("setup");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        //create a twitter client
        Client twitterClient = createTwitterClient(msgQueue);
        //create a kafka producer
        twitterClient.connect();
        //send tweets to kafka

        while (!twitterClient.isDone()){
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if(msg != null){
                logger.info(msg);
            }

        }

        logger.info("end");
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }


}
