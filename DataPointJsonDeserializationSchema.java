package tuc_stdb_flink_project;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.io.IOException;

public class DataPointJsonDeserializationSchema implements DeserializationSchema<DataPoint> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public DataPoint deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, DataPoint.class);
    }

    @Override
    public boolean isEndOfStream(DataPoint nextElement) {
        return false;
    }

    @Override
    public TypeInformation<DataPoint> getProducedType() {
        return TypeInformation.of(DataPoint.class);
    }
}
