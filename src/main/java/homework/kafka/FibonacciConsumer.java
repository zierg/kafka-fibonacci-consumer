package homework.kafka;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.function.Supplier;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class FibonacciConsumer {

    public FibonacciConsumer(String servers, int amount) {
        this(servers, amount, () -> false);
    }

    public FibonacciConsumer(String servers, int amount, Supplier<Boolean> finishCondition) {
        this.props = initProps(servers);
        this.amount = amount;
        this.finishCondition = finishCondition;
    }

    public void consume() {
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        processRecords(consumer);
        consumer.unsubscribe();
        consuming = false;
        System.out.println("Finished");
    }

    private void processRecords(KafkaConsumer<String, Long> consumer) {
        int consumed = 0;
        long sum = 0;
        while (!finishCondition.get()) {
            ConsumerRecords<String, Long> records = poll(consumer);
            for (val record : records) {
                Long value = record.value();
                sum += value;
                consumed++;
                if (consumed % amount == 0) {
                    consumed = 0;
                    log.info("Another {} records have been consumed. Total sum: {}", amount, sum);
                }
            }
        }
    }

    private ConsumerRecords<String, Long> poll(KafkaConsumer<String, Long> consumer) {
        ConsumerRecords<String, Long> poll = consumer.poll(100);
        consuming = true;
        return poll;
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

    Supplier<Boolean> finishCondition;
    int amount;
    Properties props;

    @NonFinal
    @Getter
    boolean consuming = false;

    static String TOPIC = "fibonacci";

    static Logger log = LoggerFactory.getLogger(FibonacciConsumer.class);
}
