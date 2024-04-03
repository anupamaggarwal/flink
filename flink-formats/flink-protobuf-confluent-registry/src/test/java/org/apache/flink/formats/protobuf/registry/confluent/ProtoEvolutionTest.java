package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.formats.protobuf.registry.confluent.utils.FlinkToProtoSchemaConverter;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.generated.Schema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@ExtendWith(TestLoggerExtension.class)
public class ProtoEvolutionTest {
    @Test
    public void testSchemaEvolution() {
        final RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("Name", new VarCharType(100)),
                                new RowType.RowField("salary", new IntType()),
                                new RowType.RowField("isMarried", new BooleanType())));

        Descriptors.Descriptor schemaDescriptor =
                FlinkToProtoSchemaConverter.fromFlinkSchema(
                        rowType, "row", "io.confluent.generated");
        ProtobufSchema protoSchema = new ProtobufSchema(schemaDescriptor);
        System.out.println(protoSchema);
        final RowType rowTypeUpdated =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("Name", new VarCharType(100)),
                                new RowType.RowField("Age", new IntType()),
                                new RowType.RowField("salary", new IntType()),
                                new RowType.RowField("isMarried", new BooleanType())));
        Descriptors.Descriptor schemaDescriptorUpdated =
                FlinkToProtoSchemaConverter.fromFlinkSchema(
                        rowTypeUpdated, "row", "io.confluent.generated");
        ProtobufSchema protoSchemaUpdated = new ProtobufSchema(schemaDescriptorUpdated);
        System.out.println(protoSchemaUpdated);
    }

    @Test
    public void testCompileTimeProto() throws InvalidProtocolBufferException {
        String prevByte = "CgZhbnVwYW0QChgB";
        Schema.Person foo =
                Schema.Person.newBuilder()
                        .setName("anupam")
                        .setIsMarried(true)
                        .setSalary(10)
                        .build();
        byte[] arr = foo.toByteArray();
        String encoded = Base64.getEncoder().encodeToString(arr);
        System.out.println("Encoded:" + encoded);
        byte[] arr1 = Base64.getDecoder().decode("CgZhbnVwYW0QChgB"); // migbase64
        Schema.Person deser = Schema.Person.parseFrom(arr1);
        System.out.println(deser);
    }

    @Test
    public void testMessedUpProtoWillNotWork() throws InvalidProtocolBufferException {
        String prevByte = "CgZhbnVwYW0QChgB";
        byte[] arr1 = Base64.getDecoder().decode(prevByte); // migbase64
        Schema.Person deser = Schema.Person.parseFrom(arr1);
        System.out.println(deser);
    }

    @Test
    public void testCompatProtobufWillWorkAndIgnore () throws InvalidProtocolBufferException {
        String prevByte = "CgZhbnVwYW0QChgB";
        byte[] arr1 = Base64.getDecoder().decode(prevByte); // migbase64
        Schema.Person deser = Schema.Person.parseFrom(arr1);
        System.out.println(deser);
    }

    private static void addFieldsToMap(Map<String,Integer> nameToTag , Descriptors.Descriptor descriptor){

        for(Descriptors.FieldDescriptor fdProto : descriptor.getFields()){
            if(fdProto.getType() == Descriptors.FieldDescriptor.Type.MESSAGE){
                nameToTag.put(fdProto.getFullName(),fdProto.getNumber());
                addFieldsToMap(nameToTag,fdProto.getMessageType());
            }else
                nameToTag.put(fdProto.getFullName(),fdProto.getNumber());
        }
    }

    @Test
    public void testSchemaEvolutionCompat() throws RestClientException, IOException {

        final RowType nestedRow =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("Address", new VarCharType(100)),
                                new RowType.RowField("Name", new VarCharType(100)),
                                new RowType.RowField("Contact", new IntType())));
        final RowType nestedRowUpdated =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("Address", new VarCharType(100)),
                                new RowType.RowField("Name", new VarCharType(100)),
                                new RowType.RowField("Occupation", new VarCharType(100)),
                                new RowType.RowField("Contact", new IntType())));

        final RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("Name", new VarCharType(100)),
                                new RowType.RowField("salary", new IntType()),
                                new RowType.RowField("details",nestedRow),
                                new RowType.RowField("isMarried", new BooleanType())));

        Descriptors.Descriptor schemaDescriptor =
                FlinkToProtoSchemaConverter.fromFlinkSchema(
                        rowType, "row", "io.confluent.generated");
        ProtobufSchema protoSchema = new ProtobufSchema(schemaDescriptor);
        System.out.println("GeneratedSchema:"+protoSchema);

        SchemaRegistryClient client = new MockSchemaRegistryClient();
        int schemaId = client.register("test",protoSchema);

        System.out.println(protoSchema);
        final RowType rowTypeUpdated =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("Name", new VarCharType(100)),
                                new RowType.RowField("isGraduate", new VarCharType(100)),
                                new RowType.RowField("salary", new IntType()),
                                new RowType.RowField("details",nestedRowUpdated),
                                new RowType.RowField("isMarried", new BooleanType())));
        Map<String,Integer> tagIndex = new HashMap<>();
        Descriptors.Descriptor prevDescriptor = protoSchema.toDescriptor();
        addFieldsToMap(tagIndex,prevDescriptor);
        System.out.println(tagIndex);

        Descriptors.Descriptor newDescriptor = FlinkToProtoSchemaConverter.fromFlinkSchema(rowTypeUpdated,"Row",
                "io.confluent.generated",tagIndex);
        Map<String,Integer> tagIndexNew = new HashMap<>();
        addFieldsToMap(tagIndexNew,newDescriptor);
        System.out.println(tagIndexNew);


    }
}
