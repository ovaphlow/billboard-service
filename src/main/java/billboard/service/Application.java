package billboard.service;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import kotlin.Suppress;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class Application {
  private static final Logger logger = LoggerFactory.getLogger(Application.class);

  private Server server;

  private void start(int port) throws IOException {
    server = ServerBuilder.forPort(port)
        .maxInboundMessageSize(1024 * 1024 * 256)
        .addService(new CandidateServiceImpl())
        .addService(new CommonUserServiceImpl())
        .addService(new Resume2102ServiceImpl())
        .addService(new BannerServiceImpl())
        .addService(new JobServiceImpl())
        .addService(new FavoriteServiceImpl())
        .addService(new JournalServiceImpl())
        .addService(new SendInServiceImpl())
        .addService(new DeliveryServiceImpl())
        .addService(new FeedbackServiceImpl())
        .addService(new EmployerServiceImpl())
        .addService(new MessageServiceImpl())
        .addService(new InterviewServiceImpl())
        .addService(new CampusServiceImpl())
        .addService(new TopicServiceImpl())
        .addService(new CommonUserScheduleImpl())
        .addService(new CommonDataImpl())
        .addService(new NotificationServiceImpl())
        .addService(new NotificationServiceImpl())
        .addService(new EmailServiceImpl())
        .addService(new ChartServiceImpl())
        .addService(new JobFairServiceImpl())
        .addService(new HypervisorStaffServiceImpl())
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

  @SuppressWarnings(value={"unchecked", "deprecation"})
  public static void main(String[] args) throws IOException, InterruptedException {
    try {
      Gson gson = new Gson();

      // 设置 WEB 服务地址
      String[] arg_target = args[0].split(":");
      String target = String.format("%s:%s",
          "".equals(arg_target[0]) ? "127.0.0.1" : arg_target[0],
          "".equals(arg_target[1]) ? "80" : arg_target[1]);

      final MediaType MEDIA_TYPE = MediaType.get("application/json");
      final OkHttpClient okhttp = new OkHttpClient();
      Map<String, String> body = new HashMap<>();
      body.put("token", "".equals(args[1]) ? "" : args[1]);
      Request request = new Request.Builder()
        .url("http://" + target + "/api/configuration")
        .post(RequestBody.create(MEDIA_TYPE, gson.toJson(body)))
        .build();
      try (Response response = okhttp.newCall(request).execute()) {
        if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
        String result = response.body().string();
        Map<String, String> resp = new Gson().fromJson(result, Map.class);
        Configuration.PERSISTENCE_HOST = resp.get("persistence_host");
        Configuration.PERSISTENCE_USER = resp.get("persistence_user");
        Configuration.PERSISTENCE_PASSWORD = resp.get("persistence_password");
        Configuration.PERSISTENCE_DATABASE = resp.get("persistence_database");
      }

      final Application server = new Application();
      server.start(50051);
      server.blockUntilShutdown();
    } catch (Exception e) {
      logger.error("", e);
      System.exit(0);
    }
  }
}
