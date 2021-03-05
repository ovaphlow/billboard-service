package billboard.service;

import java.sql.Connection;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

public class HypervisorStaffServiceImpl extends StaffGrpc.StaffImplBase {
  private static final Logger logger = LoggerFactory.getLogger(HypervisorStaffServiceImpl.class);

  @Override
  public void signIn(StaffSignInRequest req, StreamObserver<HypervisorReply> responseObserver) {
    String resp = "{}";
    try (Connection cnx = Persistence.getConn()) {
      String sql = """
          select id, uuid, username, name
          from mis_user
          where username = ?
            and password = ?
          """;
      Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler(),
          req.getDataMap().get("username"),
          req.getDataMap().get("password"));
      resp = new Gson().toJson(result);
    } catch (Exception e) {
      logger.error("", e);
    }
    HypervisorReply reply = HypervisorReply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
