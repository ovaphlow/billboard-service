package billboard.service;

import java.sql.Connection;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;

import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobServiceImpl extends JobGrpc.JobImplBase {
  private static final Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);

  @Override
  public void update(JobUpdateRequest req, StreamObserver<Empty> responseObserver) {
    try (Connection cnx = Persistence.getConn()) {
      if ("refresh".equals(req.getOption())) {
        String sql = """
            update recruitment set date_refresh = now() where id = ?
            """;
        new QueryRunner().execute(cnx, sql, Integer.parseInt(req.getParamMap().get("id")));
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    responseObserver.onNext(null);
    responseObserver.onCompleted();
  }
}
