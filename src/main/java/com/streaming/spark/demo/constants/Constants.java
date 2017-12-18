package com.streaming.spark.demo.constants;

public class Constants {

    public static final String SPARK_MASTER = "local[*]";
    public static final String SPARK_WAREHOUSE = "/tmp";
    public static final String STREAMING_APP_NAME = "SparkToKafkaTest";
    public static final long STREAM_BATCH_DURATION_MILLIS = 5000;
    public static final String SPARK_SERIALIZER = "org.apache.spark.serializer.KryoSerializer";
    
    public final static String KAFKA_INPUT_TOPIC = "test-input-topic";
    public final static String KAFKA_OUTPUT_TOPIC = "test-output-topic";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:6667";
    public static final String KAFKA_GROUP_ID = "test-group";
    public static final boolean KAFKA_ENABLE_AUTO_COMMIT = false;
    
    public enum APP_PROPERTIES {
        BATCH_APP_NAME("spark.batch.appname"), STREAMING_APP_NAME("spark.stream.appname"), 
        STREAM_BATCH_DURATION_MILLIS("streaming.batch.duration.millis"), SPARK_WAREHOUSE("spark.sql.warehouse.dir"), 
        SPARK_MASTER("spark.master"), SPARK_SERIALIZER("spark.serializer");
        
        private String value;

        private APP_PROPERTIES(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public enum KAFKA_PROPERTIES {
        KAFKA_SUBSCRIBE("subscribe"),
        KAFKA_BOOTSTRAP_SERVERS("bootstrap.servers"), 
        KAFKA_GROUPID("group.id"), 
        KAFKA_AUTO_OFFSET_RESET("auto.offset.reset"),
        KAFKA_ENABLE_AUTO_COMMIT("enable.auto.commit"), 
        KAFKA_KEY_DESERIALIZER("key.deserializer"), 
        KAFKA_VALUE_DESERIALIZER("value.deserializer"),
        KAFKA_KEY_SERIALIZER("key.serializer"), 
        KAFKA_VALUE_SERIALIZER("value.serializer");

        private String value;

        private KAFKA_PROPERTIES(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
