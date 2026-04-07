package org.example.service;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

import kv.grpc.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.config.TarantoolConfig;
import org.example.properties.TarantoolProperties;
import org.springframework.grpc.server.service.GrpcService;

import java.util.List;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class GrpcKvService extends KvServiceGrpc.KvServiceImplBase {
    private final TarantoolConfig tarantoolConfig;

    private final TarantoolProperties tarantoolProperties;

    private static final String PUT_METHOD = "kv_put";
    private static final String GET_METHOD = "kv_get";
    private static final String DELETE_METHOD = "kv_delete";
    private static final String RANGE_METHOD = "kv_range";
    private static final String COUNT_METHOD = "kv_count";

    @Override
    public void put(PutRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var values = List.of(
                    request.getKey(),
                    request.getValue().toByteArray()
            );
            tarantoolConfig.getClient().call(PUT_METHOD, values).get();
            log.info("Successfully inserted key: {}", request.getKey());
        }
        catch (Exception e) {
            responseObserver.onError(e);
            log.error(e.getMessage());
        }
        var empty = Empty.newBuilder().build();
        responseObserver.onNext(empty);
        responseObserver.onCompleted();
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            var values = List.of(request.getKey());
            var response = tarantoolConfig.getClient().call(GET_METHOD, values).get();

            var result = response.get();

            if (result != null && !result.isEmpty()) {
                var value = result.get(0);
                if (value instanceof byte[]) {
                    log.info("Successfully got key: {}", request.getKey());
                    responseObserver.onNext(GetResponse.newBuilder()
                            .setValue(ByteString.copyFrom((byte[]) value))
                            .build());
                    responseObserver.onCompleted();
                    return;
                }
            }

            log.info("Key not found: {}", request.getKey());
            responseObserver.onNext(GetResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to get key: {}", request.getKey(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var values = List.of(request.getKey());
            tarantoolConfig.getClient().call(DELETE_METHOD, values).get();
            log.info("Successfully deleted key: {}", request.getKey());
            var empty = Empty.newBuilder().build();
            responseObserver.onNext(empty);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to delete key: {}", request.getKey(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<KeyValue> responseObserver) {
        try {
            String currentKey = request.getKeySince();
            String keyTo = request.getKeyTo();

            while (true) {
                var values = List.of(currentKey, keyTo, tarantoolProperties.getBatchSize());

                var response = tarantoolConfig.getClient()
                        .call(RANGE_METHOD, values)
                        .get();

                var result = response.get();

                if (result == null || result.isEmpty()) {
                    break;
                }

                var items = (List<?>) result.get(0);

                if (items.isEmpty()) {
                    break;
                }

                String lastKey = null;

                for (var item : items) {
                    var pair = (List<?>) item;

                    String key = (String) pair.get(0);
                    byte[] value = (byte[]) pair.get(1);

                    responseObserver.onNext(
                            KeyValue.newBuilder()
                                    .setKey(key)
                                    .setValue(ByteString.copyFrom(value))
                                    .build()
                    );

                    lastKey = key;
                }

                if (items.size() < tarantoolProperties.getBatchSize()) {
                    break;
                }

                currentKey = lastKey + "\0";
            }

            responseObserver.onCompleted();
            log.info("Range completed: {}-{}", request.getKeySince(), keyTo);

        } catch (Exception e) {
            log.error("Range failed", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void count(Empty request, StreamObserver<CountResponse> responseObserver) {
        try {
            var response = tarantoolConfig.getClient().call(COUNT_METHOD, List.of()).get();
            var result = response.get();

            if (result != null && !result.isEmpty() && result.get(0) instanceof Number) {
                responseObserver.onNext(
                        CountResponse.newBuilder()
                                .setCount(((Number) result.get(0)).longValue())
                                .build()
                );
                responseObserver.onCompleted();
                return;
            }
            responseObserver.onNext(CountResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Count failed", e);
            responseObserver.onError(e);
        }
    }
}
