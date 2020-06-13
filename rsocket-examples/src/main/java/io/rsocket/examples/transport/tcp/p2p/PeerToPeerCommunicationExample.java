package io.rsocket.examples.transport.tcp.p2p;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

@SuppressWarnings("BusyWait")
public class PeerToPeerCommunicationExample {

  public static void main(String[] args) throws InterruptedException {
    BlockingQueue<Task> tasks = new LinkedBlockingQueue<>();
    ConcurrentMap<String, BlockingQueue<Task>> idToCompletedTasksMap = new ConcurrentHashMap<>();
    ConcurrentMap<String, RSocket> idToRSocketMap = new ConcurrentHashMap<>();

    new Thread(
            () -> {
              try {
                while (!Thread.currentThread().isInterrupted()) {
                  Task task = tasks.take();
                  Thread.sleep(2000);
                  BlockingQueue<Task> completedTasksQueue =
                      idToCompletedTasksMap.computeIfAbsent(
                          task.id, __ -> new LinkedBlockingQueue<>());

                  completedTasksQueue.offer(task);
                  RSocket rSocket = idToRSocketMap.get(task.id);
                  if (rSocket != null) {
                    rSocket
                        .fireAndForget(DefaultPayload.create(task.content))
                        .subscribe(null, e -> {}, () -> completedTasksQueue.remove(task));
                  }
                }
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            })
        .start();

    RSocketServer.create(new TasksAcceptor(tasks, idToCompletedTasksMap, idToRSocketMap))
        .bindNow(TcpServerTransport.create(9991));

    Logger logger = LoggerFactory.getLogger("RSocket.Client.ID[Test]");
    RSocket rSocketRequester =
        RSocketConnector.create()
            .setupPayload(DefaultPayload.create("Test"))
            .acceptor(
                SocketAcceptor.forFireAndForget(
                    p -> {
                      logger.info("Received Processed Task[{}]", p.getDataUtf8());
                      p.release();
                      return Mono.empty();
                    }))
            .connect(TcpClientTransport.create(9991))
            .block();

    for (int i = 0; i < 10; i++) {
      rSocketRequester.fireAndForget(DefaultPayload.create("task" + i)).block();
    }

    Thread.sleep(4000);

    rSocketRequester.dispose();
    logger.info("Disposed");

    Thread.sleep(4000);

    rSocketRequester =
        RSocketConnector.create()
            .setupPayload(DefaultPayload.create("Test"))
            .acceptor(
                SocketAcceptor.forFireAndForget(
                    p -> {
                      logger.info("Received Processed Task[{}]", p.getDataUtf8());
                      p.release();
                      return Mono.empty();
                    }))
            .connect(TcpClientTransport.create(9991))
            .block();

    logger.info("Reconnected");

    Thread.sleep(10000);
  }

  static class TasksAcceptor implements SocketAcceptor {

    static final Logger logger = LoggerFactory.getLogger(TasksAcceptor.class);

    final BlockingQueue<Task> tasksToProcess;
    final ConcurrentMap<String, BlockingQueue<Task>> idToCompletedTasksMap;
    final ConcurrentMap<String, RSocket> idToRSocketMap;

    TasksAcceptor(
        BlockingQueue<Task> tasksToProcess,
        ConcurrentMap<String, BlockingQueue<Task>> idToCompletedTasksMap,
        ConcurrentMap<String, RSocket> idToRSocketMap) {
      this.tasksToProcess = tasksToProcess;
      this.idToCompletedTasksMap = idToCompletedTasksMap;
      this.idToRSocketMap = idToRSocketMap;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
      String id = setup.getDataUtf8();
      logger.info("Accepting a new client connection with ID {}", id);
      // sendingRSocket represents here an RSocket requester to a remote peer

      if (this.idToRSocketMap.compute(
              id, (__, old) -> old == null || old.isDisposed() ? sendingSocket : old)
          == sendingSocket) {
        return Mono.<RSocket>just(
                new RSocket() {
                  @Override
                  public Mono<Void> fireAndForget(Payload payload) {
                    logger.info(
                        "Received a Task[{}] from Client.ID[{}]", payload.getDataUtf8(), id);
                    tasksToProcess.offer(new Task(id, payload.getDataUtf8()));
                    payload.release();
                    return Mono.empty();
                  }

                  @Override
                  public void dispose() {
                    idToRSocketMap.remove(id, sendingSocket);
                  }
                })
            .doOnSuccess(
                __ -> {
                  logger.info(
                      "Accepted a new client connection with ID {}. Checking for remaining tasks",
                      id);
                  BlockingQueue<Task> tasksToDeliver = this.idToCompletedTasksMap.get(id);

                  if (tasksToDeliver == null || tasksToDeliver.isEmpty()) {
                    // means nothing yet to send
                    return;
                  }

                  logger.info("Found remaining tasks to deliver for client {}", id);

                  for (; ; ) {
                    Task task = tasksToDeliver.poll();

                    if (task == null) {
                      return;
                    }

                    sendingSocket
                        .fireAndForget(DefaultPayload.create(task.content))
                        .subscribe(
                            null,
                            e -> {
                              // offers back a task if it has not been delivered
                              tasksToDeliver.offer(task);
                            });
                  }
                });
      }

      return Mono.error(
          new IllegalStateException("There is already a client connected with the same ID"));
    }
  }

  static class Task {
    final String id;
    final String content;

    Task(String id, String content) {
      this.id = id;
      this.content = content;
    }
  }
}
