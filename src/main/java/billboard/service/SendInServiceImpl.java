package billboard.service;

import java.sql.Connection;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

public class SendInServiceImpl extends SendInGrpc.SendInImplBase {
  private static final Logger logger = LoggerFactory.getLogger(SendInServiceImpl.class);

  @Override
  public void statistic(SendInStatisticRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "{}";
    try (Connection cnx = Persistence.getConn()) {
      if ("hypervisor-all".equals(req.getOption())) {
        String sql = "select count(*) as qty from delivery";
        Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler());
        resp = new Gson().toJson(result);
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    BizReply reply = BizReply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
