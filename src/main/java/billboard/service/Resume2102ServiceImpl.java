package billboard.service;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
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

  @Override
  public void filter(Resume2102FilterRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "[]";
    try (Connection cnx = Persistence.getConn()) {
      if ("employer-filter".equals(req.getFilter())) {
        int page = Integer.parseInt(req.getParamMap().get("page")) | 0;
        int offset = page > 0 ? page * 20 : 0;
        String sql = """
            select *
            from resume
            where education = ?
              and status = '公开'
              and position(? in address2) > 0
              and position(? in qiwanghangye) > 0
              and position(? in qiwangzhiwei) > 0
            order by date_update desc
            limit ?, 20
            """;
        List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler(),
            req.getParamMap().get("education"),
            req.getParamMap().get("address2"),
            req.getParamMap().get("qiwanghangye"),
            req.getParamMap().get("qiwangzhiwei"),
            offset);
        resp = new Gson().toJson(result);
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    BizReply reply = BizReply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void update(Resume2102UpdateRequest req, StreamObserver<Empty> responseObserver) {
    try (Connection cnx = Persistence.getConn()) {
      if ("refresh".equals(req.getOption())) {
        String sql = """
            update resume set date_update = now() where common_user_id = ?
            """;
        new QueryRunner().execute(cnx, sql, Integer.parseInt(req.getParamMap().get("candidate_id")));
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    responseObserver.onNext(null);
    responseObserver.onCompleted();
  }
}
