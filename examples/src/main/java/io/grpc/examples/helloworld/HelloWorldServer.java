/*
 * Copyright 2015, Google Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 *    * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.grpc.examples.helloworld;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import io.grpc.Attributes;
import io.grpc.Attributes.Key;
import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerTransportFilter;
import io.grpc.stub.StreamObserver;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class HelloWorldServer {
  private static final Logger logger = Logger.getLogger(HelloWorldServer.class.getName());

  private Server server;

  static Context.Key<String>    CONTEXT_CONVERSATION_ID = Context.key("convid");
  static Attributes.Key<String> ATTRIBUTES_CONVERSATION_ID = Key.of("convid");

  static class ConvIdServerInterceptor implements ServerInterceptor {
      @Override
      public <ReqT, RespT> Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call, final Metadata headers,
              final ServerCallHandler<ReqT, RespT> next) {
          Context context = Context.current().withValue(CONTEXT_CONVERSATION_ID, call.getAttributes().get(ATTRIBUTES_CONVERSATION_ID));
          return Contexts.interceptCall(context, call, headers, next);
      }
  }

  static class ConvIdServerTransportFilter extends ServerTransportFilter {
      AtomicLong id = new AtomicLong(0);

      @Override
      public Attributes transportReady(Attributes transportAttrs) {
          Attributes myAttributes = Attributes.newBuilder(transportAttrs).set(ATTRIBUTES_CONVERSATION_ID, "my_conversation_" + (id.incrementAndGet())).build();
          return super.transportReady(myAttributes);
      }

      @Override
      public void transportTerminated(Attributes transportAttrs) {
          System.out.println("Conversation " + transportAttrs.get(ATTRIBUTES_CONVERSATION_ID) + " has been disconnected!");
          super.transportTerminated(transportAttrs);
      }
  }

  private void start() throws IOException {
    /* The port on which the server should run */
    int port = 50051;
    BindableService bs = new GreeterImpl();

    server = ServerBuilder.forPort(port)
        .addService(ServerInterceptors.intercept(new GreeterImpl(), new ConvIdServerInterceptor()))
        .addTransportFilter(new ConvIdServerTransportFilter())
        .build()
        .start();

    logger.info("yop Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        HelloWorldServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    final HelloWorldServer server = new HelloWorldServer();
    server.start();
    server.blockUntilShutdown();
  }

  static class GreeterImpl extends GreeterGrpc.GreeterImplBase {

    @Override
    public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
      System.out.println("Conversation " + CONTEXT_CONVERSATION_ID.get() + " is saying hello!");

      HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + req.getName()).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }
}
