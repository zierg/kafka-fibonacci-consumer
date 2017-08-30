package test.homework.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import homework.kafka.FibonacciConsumer;
import kafka.server.KafkaServer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class FibonacciConsumerTest {

    @Test
    public void test() {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FibonacciConsumer.class);
        root.setLevel(Level.INFO);
        TestAppender appender = new TestAppender();
        root.addAppender(appender);
        FibonacciConsumer consumer = startConsuming(appender);
        sendValues();
        stopConsuming(consumer);
        root.detachAppender(appender);
        System.out.println("APPENDER: " + appender.getEvents());
        embeddedKafka.getKafkaServers().forEach(KafkaServer::shutdown);
        embeddedKafka.getZookeeper().shutdown();


//        Logger logger = Logger.getLogger(FibonacciConsumer.class);
//        TestAppender appender = new TestAppender();
//        logger.addAppender(appender);
//
    }

    private FibonacciConsumer startConsuming(TestAppender appender) {
        FibonacciConsumer consumer = new FibonacciConsumer(embeddedKafka.getBrokersAsString(), PRINTING_NUMBER);
        new Thread(consumer::consume).start();
        long before = System.currentTimeMillis();
        //noinspection StatementWithEmptyBody
        while (System.currentTimeMillis() - before < 2000 && appender.getEvents().size() < 5);
//        consumer.consume();
        return consumer;
    }

    private void sendValues() {
        Map<String, Object> producerProperties = getProducerProperties();
        KafkaProducer<String, Long> producer = new KafkaProducer<>(producerProperties);
        for (val value : INPUT) {
            producer.send(new ProducerRecord<>(TOPIC, value));
        }
    }

    private Map<String, Object> getProducerProperties() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        return producerProps;
    }

    private void stopConsuming(FibonacciConsumer consumer) {
        consumer.setWorking(false);
    }

    static Long[] INPUT = {1L, 1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L};
    static Long[] OUTPUT = {2L, 7L, 20L, 54L, 143L};

    static int PRINTING_NUMBER = INPUT.length / OUTPUT.length;

    static int TIME_LIMIT = 5000;
    static String TOPIC = "fibonacci";

    @ClassRule
    public static final KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, 1, TOPIC);



//    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
//    class TestAppender extends AppenderSkeleton {
//
//        @Override
//        protected void append(LoggingEvent event) {
//            events.add(event);
//        }
//
//        @Override
//        public void close() {
//
//        }
//
//        @Override
//        public boolean requiresLayout() {
//            return false;
//        }
//
//        @Getter
//        List<LoggingEvent> events = new ArrayList<>();
//    }
}