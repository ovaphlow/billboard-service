package billboard.service;

import com.google.gson.Gson;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class FeedbackServiceImpl extends FeedbackGrpc.FeedbackImplBase {
  private static Logger logger = LoggerFactory.getLogger(FeedbackServiceImpl.class);

  /**
   * 2021-03: 未整理
   */
  @Override
  public void insert(FeedbackInsertRequest req, StreamObserver<MiscReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "insert into feedback (user_id, user_uuid, user_category, content, datime, category, status) value (?, ?, ?, ?, ?, ?, '未处理')";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getUserId());
        ps.setString(2, req.getUserUuid());
        ps.setString(3, req.getUserCategory());
        ps.setString(4, req.getContent());
        ps.setString(5, req.getDatime());
        ps.setString(6, req.getCategory());
        ps.execute();
        resp.put("content", true);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    MiscReply reply = MiscReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void filter(FeedbackFilterRequest req, StreamObserver<MiscReply> responseObserver) {
    String resp = "[]";
    try (Connection cnx = Persistence.getConn()) {
      if ("hypervisor".equals(req.getOption())) {
        String sql = """
            select *
            from feedback
            where position(? in category) > 0
              and position(? in user_category) > 0
              and position(? in status) > 0
            order by id desc
            limit 100""";
        List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler(),
            req.getDataMap().get("category"),
            req.getDataMap().get("user_category"),
            req.getDataMap().get("status"));
        resp = new Gson().toJson(result);
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    MiscReply reply = MiscReply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  /**
   * 2021-03: 未整理
   */
  @Override
  public void list(FeedbackListRequest req, StreamObserver<MiscReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from feedback where user_id = ? and user_category = ? ORDER BY datime DESC ";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getUserId());
        ps.setString(2, req.getUserCategory());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    MiscReply reply = MiscReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void reply(FeedbackReplyRequest req, StreamObserver<Empty> responseObserver) {
    try (Connection cnx = Persistence.getConn()) {
      String sql = """
          update feedback
          set status = '已处理'
          where id = ?
          """;
      new QueryRunner().execute(cnx, sql);
    } catch (Exception e) {
      logger.error("", e);
    }
    responseObserver.onNext(null);
    responseObserver.onCompleted();
  }
}
