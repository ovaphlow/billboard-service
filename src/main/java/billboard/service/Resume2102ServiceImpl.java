package billboard.service;

import java.sql.Connection;

import com.google.gson.Gson;

import io.grpc.stub.StreamObserver;

import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Resume2102ServiceImpl extends Resume2102Grpc.Resume2102ImplBase {
  private static final Logger logger = LoggerFactory.getLogger(Resume2102ServiceImpl.class);

  @Override
  public void init(Resume2102InitRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "";
    try (Connection cnx = Persistence.getConn()) {
      String sql = """
          insert into resume (
            common_user_id, uuid, ziwopingjia, career, record
          )
          value (
            ?, uuid(), '', '[]', '[]'
          )
          """;
      new QueryRunner().execute(cnx, sql, req.getCandidateId());
    } catch (Exception e) {
      logger.error("", e);
    }
    BizReply reply = BizReply.newBuilder().setData(new Gson().toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
