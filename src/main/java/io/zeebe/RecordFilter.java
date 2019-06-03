package io.zeebe;

import io.zeebe.exporter.api.context.Context;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;

public class RecordFilter implements Context.RecordFilter {

    @Override
    public boolean acceptType(RecordType recordType) {
        return true;
    }

    @Override
    public boolean acceptValue(ValueType valueType) {
        return valueType.equals(ValueType.JOB);
//        return !valueType.equals(ValueType.JOB_BATCH);
    }
}
