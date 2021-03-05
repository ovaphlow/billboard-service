package billboard.service;

import java.sql.Connection;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

public class CandidateServiceImpl extends CandidateGrpc.CandidateImplBase {
  private static final Logger logger = LoggerFactory.getLogger(CandidateServiceImpl.class);

  @Override
  public void statistics(CandidateStatisticsRequest req,
      StreamObserver<BizReply> responseObserver) {
    String resp = "";
    try (Connection cnx = Persistence.getConn()) {
      if ("hypervisor-all".equals(req.getOption())) {
        String sql = "select count(*) as qty from common_user";
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
