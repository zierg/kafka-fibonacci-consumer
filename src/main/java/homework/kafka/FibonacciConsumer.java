package homework.kafka;

import lombok.AccessLevel;
import lombok.Setter;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class FibonacciConsumer {

    public FibonacciConsumer(String servers, int amount) {
        this.props = initProps(servers);
        this.amount = amount;
    }

    public void consume() {
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        int consumed = 0;
        long sum = 0;
        working = true;
        while (working) {
            ConsumerRecords<String, Long> records = consumer.poll(100);
            for (val record : records) {
                System.out.println("consumed");
                Long value = record.value();
                sum += value;
                consumed++;
                if (consumed % amount == 0) {
                    consumed = 0;
                    log.info("Another {} records have been consumed. Total sum: {}", amount, sum);
                }
            }
        }
        System.out.println("Finished");
    }

    private Properties initProps(String servers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", "fibonacci");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        return props;
    }

    @Setter
    @NonFinal
    boolean working = false;

    int amount;
    Properties props;

    static String TOPIC = "fibonacci";

    static Logger log = LoggerFactory.getLogger(FibonacciConsumer.class);
}
