package homework.kafka;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ConsumerMain {

    public static void main(String[] args) {
        int printingNumber = getPrintingNumber(args);
        new FibonacciConsumer("localhost:9092", printingNumber).consume();
    }

    private static int getPrintingNumber(String[] args) {
        if (args.length == 0) {
            log.info("No Fibonacci printing number has been provided. The default value {} will be used.", DEFAULT_PRINTING_NUMBER);
            return DEFAULT_PRINTING_NUMBER;
        }
        try {
            return Integer.parseInt(args[0]);
        } catch (NumberFormatException ex) {
            log.warn("The provided value '{}' is not a valid integer. The default value {} will be used.", args[0], DEFAULT_PRINTING_NUMBER);
            return DEFAULT_PRINTING_NUMBER;
        }
    }

    static int DEFAULT_PRINTING_NUMBER = 2;

    static Logger log = LoggerFactory.getLogger(ConsumerMain.class);
}
