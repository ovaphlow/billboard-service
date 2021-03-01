package billboard.service;

import com.google.gson.Gson;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResumeServiceImpl extends ResumeGrpc.ResumeImplBase {
  private static final Logger logger = LoggerFactory.getLogger(ResumeServiceImpl.class);

  @Override
  public void status(ResumeStatusRequest req, StreamObserver<ResumeReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "update resume set status=? where common_user_id=? and uuid=?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getStatus());
        ps.setInt(2, req.getId());
        ps.setString(3, req.getUuid());
        ps.execute();
        resp.put("content", true);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    ResumeReply reply = ResumeReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void check(ResumeCheckRequest req, StreamObserver<ResumeReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from resume where common_user_id = ? ";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getId());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() == 0) {
          resp.put("message", "请完善简历个人信息");
        } else {
          Map<String, Object> map = result.get(0);
          if ((map.get("name") == null || "".equals(map.get("name")))
              || (map.get("phone") == null || "".equals(map.get("phone")))
              || (map.get("email") == null || "".equals(map.get("email")))
              || (map.get("gender") == null || "".equals(map.get("gender")))
              || (map.get("birthday") == null || "".equals(map.get("birthday")))) {
            resp.put("content", false);
          } else {
            resp.put("content", true);
          }
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    ResumeReply reply = ResumeReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
