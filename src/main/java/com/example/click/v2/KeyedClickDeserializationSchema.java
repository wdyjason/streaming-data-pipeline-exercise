package com.example.click.v2;

import com.example.click.Click;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KeyedClickDeserializationSchema implements KafkaDeserializationSchema<Click> {

    @Override
    public boolean isEndOfStream(Click nextElement) {
        return false;
    }

    @Override
    public Click deserialize(ConsumerRecord<byte[], byte[]> record) {
        String key = new String(record.key());
        long value = Long.parseLong(new String(record.value()));

        return new Click(key, value);
    }

    @Override
    public TypeInformation<Click> getProducedType() {
        return TypeInformation.of(Click.class);
    }
}
