package org.sagebionetworks.bridge.udd.helper;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Custom serializer for Joda LocalDate, because the one in jackson-datatype-joda serializes to a weird format. This
 * one serializes to a string in a simple YYYY-MM-DD format.
 */
// TODO: This is copy-pasted from BridgePF. We should refactor this into a common shared lib
public class LocalDateToStringSerializer extends JsonSerializer<LocalDate> {
    /** {@inheritDoc} */
    @Override
    public void serialize(LocalDate date, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeString(date.toString(ISODateTimeFormat.date()));
    }
}
