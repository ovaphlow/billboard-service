package billboard.service;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Application {
  private static final Logger logger = LoggerFactory.getLogger(Application.class);

  private Server server;

  private void start() throws IOException {
    int port = 5001;
    server = ServerBuilder.forPort(port)
        .maxInboundMessageSize(1024 * 1024 * 256)
        .addService(new DemoServiceImpl())
        .addService(new CommonUserServiceImpl())
        .addService(new CommonUserFileServiceImpl())
        .addService(new ResumeServiceImpl())
        .addService(new Resume2102ServiceImpl())
        .addService(new BannerServiceImpl())
        .addService(new RecruitmentServiceImpl())
        .addService(new FavoriteServiceImpl())
        .addService(new JournalServiceImpl())
        .addService(new DeliveryServiceImpl())
        .addService(new FeedbackServiceImpl())
        .addService(new ReportServiceImpl())
        .addService(new EnterpriseServiceImpl())
        .addService(new EnterpriseUserServiceImpl())
        .addService(new MessageServiceImpl())
        .addService(new OfferServiceImpl())
        .addService(new CampusServiceImpl())
        .addService(new TopicServiceImpl())
        .addService(new CommonUserScheduleImpl())
        .addService(new CommonDataImpl())
        .addService(new RecommendServiceImpl())
        .addService(new EmailServiceImpl())
        .addService(new ChartServiceImpl())
        .addService(new JobFairServiceImpl())
        .build()
        .start();
    logger.info("服务启动于端口 " + port);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.err.println("*** shutting down gRPC server since JVM is shutting down");
      Application.this.stop();
      System.err.println("*** server shut down");
    }));
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    final Application server = new Application();
    server.start();
    server.blockUntilShutdown();
  }
}
