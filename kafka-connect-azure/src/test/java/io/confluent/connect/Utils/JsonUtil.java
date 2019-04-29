package io.confluent.connect.Utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class JsonUtil {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Collection<Object> getRecords(InputStream in) throws IOException {
        JsonParser reader = mapper.getFactory().createParser(in);

        ArrayList<Object> records = new ArrayList<>();
        Iterator<Object> iterator = reader.readValuesAs(Object.class);
        while (iterator.hasNext()) {
            records.add(iterator.next());
        }
        return records;
    }
}
