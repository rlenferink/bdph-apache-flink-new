import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FlinkTwitter {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkTwitter.class);

    private static final String CFG_DEBUG = "debug";

    public static void main(String[] args){

        if (args.length < 1) {
            System.out.println(
                    "FlinkTwitter\n\n" +
                            "Usage:\n" +
                            "\tjava -jar {filename}.jar <config file> \n");
            System.exit(1);
        }

        File cfgFile = new File(args[0]);
        if (!cfgFile.exists()){
            System.out.format("Loading config file '%s' failed !%n", args[0]);
            System.exit(1);
        }

        Properties cfg = new Properties();
        InputStream input;

        try {
            input = new FileInputStream(cfgFile);
            cfg.load(input);
        } catch (IOException e) {
            System.out.format("Loading config file '%s' failed !%n", args[0]);
            System.exit(1);
        }

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "Vhh9wtoeqzWf08BhwmeUXOTSB");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "Ou1nHWLvIoxQE56mYDJl5JauwkY2N67NSfQYyO46MYjuYDnKIJ");
        props.setProperty(TwitterSource.TOKEN, "3221388387-VwffqrtFc3P0fHVDZPL8ZxpCuaSUguw2rbGoz23");
        props.setProperty(TwitterSource.TOKEN_SECRET, "7dhijHUDA4BBtnCxAdnQqlLiNPWVF2P3jRMmWTrKW2oJ4");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env
                .addSource(new TwitterSource(props));

        inputStream.print();

        try {
            env.execute("Flink twitter consumer");
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info("End reached");
    }

}
