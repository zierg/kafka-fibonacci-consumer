package test.homework.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import homework.kafka.FibonacciConsumer;
import kafka.server.KafkaServer;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class FibonacciConsumerTest {

    @Test
    public void test() throws InterruptedException, ExecutionException {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FibonacciConsumer.class);
        root.setLevel(Level.INFO);
        TestAppender.getEvents().clear();
        TestAppender appender = new TestAppender();
        root.addAppender(appender);
        consume();
        root.detachAppender(appender);
        embeddedKafka.getKafkaServers().forEach(KafkaServer::shutdown);
        embeddedKafka.getZookeeper().shutdown();
        checkLog();
    }

    private void consume() throws InterruptedException, ExecutionException {
        FibonacciConsumer consumer = new FibonacciConsumer(embeddedKafka.getBrokersAsString(),
                                                           PRINTING_NUMBER,
                                                           getFinishingCondition(System.currentTimeMillis()));
        new Thread(consumer::consume).start();
        await().atMost(TIME_BEFORE_SENDING, TimeUnit.SECONDS).until(consumer::isConsuming);
        sendValues();
    }

    private Supplier<Boolean> getFinishingCondition(long timeBefore) {
        return () -> TestAppender.getEvents().size() >= 5
                || System.currentTimeMillis() - timeBefore >= TIME_BEFORE_FINISHING_CONSUMPTION;
    }

    private void sendValues() throws ExecutionException, InterruptedException {
        Map<String, Object> producerProperties = getProducerProperties();
        KafkaProducer<String, Long> producer = new KafkaProducer<>(producerProperties);
        for (val value : INPUT) {
            producer.send(new ProducerRecord<>(TOPIC, value)).get();
        }
    }

    private Map<String, Object> getProducerProperties() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        return producerProps;
    }

    private void checkLog() {
        List<ILoggingEvent> events = TestAppender.getEvents();
        assertThat(events).hasSize(OUTPUT.length);
        for (int i = 0; i < events.size(); i++) {
            ILoggingEvent event = events.get(i);
            assertThat(event)
                    .returns(Level.INFO, ILoggingEvent::getLevel)
                    .returns(LOG_MESSAGE, ILoggingEvent::getMessage);

            Object[] logArgs = event.getArgumentArray();
            assertThat(logArgs)
                    .containsExactly(PRINTING_NUMBER, OUTPUT[i]);
        }
    }

    static Long[] INPUT = {1L, 1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L};
    static Long[] OUTPUT = {2L, 7L, 20L, 54L, 143L};

    static int PRINTING_NUMBER = 2;

    static int TIME_BEFORE_SENDING = 5;
    static int TIME_BEFORE_FINISHING_CONSUMPTION = 10000;
    static String TOPIC = "fibonacci";

    @ClassRule
    public static final KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, 1, TOPIC);
    private static final String LOG_MESSAGE = "Another {} records have been consumed. Total sum: {}";
}