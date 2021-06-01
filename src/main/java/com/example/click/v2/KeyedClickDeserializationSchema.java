package com.example.click.v2;

import com.example.click.RawClick;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KeyedClickDeserializationSchema implements KafkaDeserializationSchema<RawClick> {

    @Override
    public boolean isEndOfStream(RawClick nextElement) {
        return false;
    }

    @Override
    public RawClick deserialize(ConsumerRecord<byte[], byte[]> record) {
        String key = new String(record.key());
        long value = Long.parseLong(new String(record.value()));

        return new RawClick(key, value);
    }

    @Override
    public TypeInformation<RawClick> getProducedType() {
        return TypeInformation.of(RawClick.class);
    }
}
