/*
 * Copyright JAMF Software, LLC
 */

package com.jamf.regatta.core.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.jamf.regatta.core.RetryConfig;
import com.jamf.regatta.core.api.ByteSequence;
import com.jamf.regatta.core.api.GetResponse;
import com.jamf.regatta.core.encoding.SnappyCodec;
import com.jamf.regatta.core.options.DeleteOption;
import com.jamf.regatta.core.options.GetOption;
import com.jamf.regatta.proto.DeleteRangeRequest;
import com.jamf.regatta.proto.DeleteRangeResponse;
import com.jamf.regatta.proto.KVGrpc;
import com.jamf.regatta.proto.KeyValue;
import com.jamf.regatta.proto.PutRequest;
import com.jamf.regatta.proto.PutResponse;
import com.jamf.regatta.proto.RangeRequest;
import com.jamf.regatta.proto.RangeResponse;

import io.grpc.Codec;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

class KVImplTest {

	private static final ByteSequence TABLE = ByteSequence.fromUtf8String("table1");
	private static final ByteSequence KEY = ByteSequence.fromUtf8String("key");
	private static final ByteSequence VALUE = ByteSequence.fromUtf8String("value");

	private static final KeyValue PROTO_KV = KeyValue.newBuilder()
			.setKey(ByteString.copyFromUtf8("key"))
			.setValue(ByteString.copyFromUtf8("value"))
			.setCreateRevision(3)
			.setModRevision(7)
			.build();

	private final KVTestStub service = new KVTestStub();

	private Server server;
	private ManagedChannel channel;
	private KVImpl client;

	@BeforeEach
	void setUp() throws IOException {
		// The KV client always uses snappy compression, so both ends need the codec registered.
		var compressorRegistry = CompressorRegistry.newEmptyInstance();
		compressorRegistry.register(Codec.Identity.NONE);
		compressorRegistry.register(SnappyCodec.INSTANCE);
		var decompressorRegistry = DecompressorRegistry.emptyInstance()
				.with(Codec.Identity.NONE, false)
				.with(SnappyCodec.INSTANCE, true);

		var serverName = InProcessServerBuilder.generateName();
		server = InProcessServerBuilder.forName(serverName)
				.directExecutor()
				.compressorRegistry(compressorRegistry)
				.decompressorRegistry(decompressorRegistry)
				.addService(service)
				.build()
				.start();
		channel = InProcessChannelBuilder.forName(serverName)
				.directExecutor()
				.compressorRegistry(compressorRegistry)
				.decompressorRegistry(decompressorRegistry)
				.build();
		client = new KVImpl(channel, RetryConfig.NO_RETRY);
	}

	@AfterEach
	void tearDown() {
		channel.shutdownNow();
		server.shutdownNow();
	}

	@Test
	void get_mapsRevisions() {
		var result = client.get(TABLE, KEY);

		assertThat(result.kvs()).containsExactly(
				new com.jamf.regatta.core.api.KeyValue(KEY, VALUE, 3, 7));
	}

	@Test
	void get_defaultOptionSendsNoRevisionBounds() {
		client.get(TABLE, KEY);

		assertThat(service.rangeRequests).singleElement().satisfies(request -> {
			assertThat(request.getMinModRevision()).isZero();
			assertThat(request.getMaxModRevision()).isZero();
			assertThat(request.getMinCreateRevision()).isZero();
			assertThat(request.getMaxCreateRevision()).isZero();
		});
	}

	@Test
	void get_sendsRevisionBounds() {
		client.get(TABLE, KEY, revisionBounds());

		assertThat(service.rangeRequests).singleElement().satisfies(KVImplTest::assertRevisionBounds);
	}

	@Test
	void iterate_mapsRevisionsAndSendsRevisionBounds() {
		var result = client.iterate(TABLE, KEY, revisionBounds())
				.map(GetResponse::kvs)
				.flatMap(List::stream)
				.toList();

		assertThat(result).containsExactly(
				new com.jamf.regatta.core.api.KeyValue(KEY, VALUE, 3, 7));
		assertThat(service.iterateRequests).singleElement().satisfies(KVImplTest::assertRevisionBounds);
	}

	@Test
	void put_mapsPrevKvRevisions() {
		var result = client.put(TABLE, KEY, VALUE);

		assertThat(result.prev()).isEqualTo(new com.jamf.regatta.core.api.KeyValue(KEY, VALUE, 3, 7));
	}

	@Test
	void delete_mapsPrevKvsRevisions() {
		var result = client.delete(TABLE, KEY, DeleteOption.builder().withPrevKV(true).build());

		assertThat(result.prevKv()).containsExactly(
				new com.jamf.regatta.core.api.KeyValue(KEY, VALUE, 3, 7));
	}

	private static GetOption revisionBounds() {
		return GetOption.builder()
				.withMinModRevision(10)
				.withMaxModRevision(20)
				.withMinCreateRevision(1)
				.withMaxCreateRevision(5)
				.build();
	}

	private static void assertRevisionBounds(RangeRequest request) {
		assertThat(request.getMinModRevision()).isEqualTo(10);
		assertThat(request.getMaxModRevision()).isEqualTo(20);
		assertThat(request.getMinCreateRevision()).isEqualTo(1);
		assertThat(request.getMaxCreateRevision()).isEqualTo(5);
	}

	private static class KVTestStub extends KVGrpc.KVImplBase {

		private final List<RangeRequest> rangeRequests = new CopyOnWriteArrayList<>();
		private final List<RangeRequest> iterateRequests = new CopyOnWriteArrayList<>();

		@Override
		public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
			rangeRequests.add(request);
			responseObserver.onNext(RangeResponse.newBuilder().addKvs(PROTO_KV).setCount(1).build());
			responseObserver.onCompleted();
		}

		@Override
		public void iterateRange(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
			iterateRequests.add(request);
			responseObserver.onNext(RangeResponse.newBuilder().addKvs(PROTO_KV).setCount(1).build());
			responseObserver.onCompleted();
		}

		@Override
		public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
			responseObserver.onNext(PutResponse.newBuilder().setPrevKv(PROTO_KV).build());
			responseObserver.onCompleted();
		}

		@Override
		public void deleteRange(DeleteRangeRequest request, StreamObserver<DeleteRangeResponse> responseObserver) {
			responseObserver.onNext(DeleteRangeResponse.newBuilder().addPrevKvs(PROTO_KV).setDeleted(1).build());
			responseObserver.onCompleted();
		}
	}
}
