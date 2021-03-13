package billboard.service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FairServiceImpl extends FairGrpc.FairImplBase {
  private static final Logger logger = LoggerFactory.getLogger(FairServiceImpl.class);

  @Override
  public void list(FairListRequest req, StreamObserver<BulletinReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql =
        " select * from job_fair as j "
      + " where status = '启用' order by datime desc ";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BulletinReply reply = BulletinReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }


  @Override
  public void get(FairGetRequest req, StreamObserver<BulletinReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from job_fair where  id = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getId());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() == 0) {
          resp.put("message", "该招聘会已不存在");
        } else {
          resp.put("content", result.get(0));
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BulletinReply reply = BulletinReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void search(FairSearchRequest req, StreamObserver<BulletinReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select *,"
          + " (select count(*) from recruitment "
          + " where json_search(job_fair_id, \"one\", j.id) and enterprise_id = ? and enterprise_uuid = ?) as qty"
          + " from job_fair as j where status = '启用' order by datime desc ";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getEntId());
        ps.setString(2, req.getEntUuid());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BulletinReply reply = BulletinReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void update(FairUpdateRequest req, StreamObserver<BulletinReply> responseObserver) {
    logger.info("{}", req);
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String cleanSql =
      " update recruitment " +
      " set job_fair_id = json_remove(job_fair_id,replace(JSON_SEARCH(job_fair_id,'one',concat(?,'')) ,'\"',''))" +
      " where enterprise_id = ? and enterprise_uuid = ?";
      try (PreparedStatement ps = conn.prepareStatement(cleanSql)) {
        ps.setInt(1, req.getJobFairId());
        ps.setInt(2, req.getEntId());
        ps.setString(3, req.getEntUuid());
        ps.execute();
      }
      if (!"[]".equals(req.getRecruitmentId())) {
        String sql =
        " update recruitment " +
        " set job_fair_id = if(job_fair_id is null, json_array(concat(?,'')) ,json_array_append(job_fair_id,'$',concat(?,'')))" +
        " where id in (";
        List<Integer> recruitmentList = gson.fromJson(req.getRecruitmentId(),
        new TypeToken<List<Integer>>(){}.getType());
        for (int i = 0; i < recruitmentList.size(); i++){
          sql += "?,";
        }
        sql=sql.substring(0, sql.length()-1);
        sql += ')';
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setInt(1, req.getJobFairId());
          ps.setInt(2, req.getJobFairId());
          int inx = 3;
          for (Integer item: recruitmentList) {
            ps.setInt(inx, item.intValue());
            inx += 1;
          }
          ps.execute();
        }
      }
      resp.put("content", "");
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BulletinReply reply = BulletinReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

}
