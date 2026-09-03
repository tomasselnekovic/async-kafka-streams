package kafka.streams.async.spring;

import kafka.streams.async.AsyncMetricsListener;
import kafka.streams.async.MicrometerAsyncMetricsListener;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(AsyncKafkaStreamsProperties.class)
public class AsyncKafkaStreamsAutoConfiguration {
    @Bean(name = "asyncKafkaStreamsExecutor")
    @ConditionalOnMissingBean(name = "asyncKafkaStreamsExecutor")
    public Executor asyncKafkaStreamsExecutor(AsyncKafkaStreamsProperties properties) {
        return Executors.newFixedThreadPool(properties.getExecutorThreads(), runnable -> {
            Thread t = new Thread(runnable);
            t.setName("async-kafka-streams-" + t.getId());
            t.setDaemon(true);
            return t;
        });
    }

    @Bean
    @ConditionalOnMissingBean(AsyncMetricsListener.class)
    public AsyncMetricsListener noopAsyncMetricsListener() {
        return AsyncMetricsListener.noop();
    }

    @Bean
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(name = "micrometerAsyncMetricsListener")
    public AsyncMetricsListener micrometerAsyncMetricsListener(
            MeterRegistry registry,
            AsyncKafkaStreamsProperties properties
    ) {
        return new MicrometerAsyncMetricsListener(registry, properties.getProcessorName());
    }
}
