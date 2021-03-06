package billboard.service;

import java.sql.Connection;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

public class EmployerServiceImpl extends EmployerGrpc.EmployerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(EmployerServiceImpl.class);

  @Override
  public void statistic(EmployerStatisticRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "{}";
    try (Connection cnx = Persistence.getConn()) {
      if ("hypervisor-all".equals(req.getOption())) {
        String sql = "select count(*) as qty from enterprise";
        Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler());
        resp = new Gson().toJson(result);
      } else if ("hypervisor-today".equals(req.getOption())) {
        String sql = """
            select count(*) as qty
            from enterprise
            where position(? in date) > 0
            """;
        Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler(),
            req.getDataMap().get("date"));
        resp = new Gson().toJson(result);
      } else if ("hypervisor-certificate".equals(req.getOption())) {
        String sql = "select count(*) as qty from enterprise where status = '待认证'";
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
