package billboard.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

public class EmployerServiceImpl extends EmployerGrpc.EmployerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(EmployerServiceImpl.class);

  @Override
  public void filter(EmployerFilterRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "[]";
    try (Connection cnx = Persistence.getConn()) {
      if ("hypervisor-certificate-filter".equals(req.getOption())) {
        String sql = """
            select id, uuid, name, faren, phone, status
            from enterprise
            where position(? in name) > 0
              and status = '待认证'
            order by id desc
            """;
        List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler(),
            req.getDataMap().get("name"));
        resp = new Gson().toJson(result);
      } else if ("hypervisor".equals(req.getOption())) {
        String sql = """
            select id, uuid, name, phone
            from enterprise
            where position(? in name) > 0
              or position(? in phone) > 0
            order by id desc
            limit 100
            """;
        List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler(),
            req.getDataMap().get("keyword"),
            req.getDataMap().get("keyword"));
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
  public void filterUser(EmployerFilterUserRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "[]";
    try (Connection cnx = Persistence.getConn()) {
      if ("by-employer-id-list".equals(req.getOption())) {
        String sql = """
            select id, uuid, enterprise_id, enterprise_uuid, email, phone, name
            from enterprise_user
            where enterprise_id in (%s)
            """;
        sql = String.format(sql, req.getDataMap().get("list"));
        List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler());
        resp = new Gson().toJson(result);
      } else if ("by-id-list".equals(req.getOption())) {
        String sql = """
            select id, uuid, enterprise_id, enterprise_uuid, email, phone, name
            from enterprise_user
            where id in (%s)
            """;
        sql = String.format(sql, req.getDataMap().get("list"));
        List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler());
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

  @Override
  public void get(EmployerGetRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select e.*,u.id as ent_user_id from enterprise e "
          + "left join enterprise_user u on e.id = u.enterprise_id  where e.id = ? and (u.uuid=? or e.uuid=?)  ";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getId());
        ps.setString(2, req.getUuid());
        ps.setString(3, req.getUuid());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() == 0) {
          resp.put("message", "该企业已不存在");
        } else {
          resp.put("content", result.get(0));
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BizReply reply = BizReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void check(EmployerCheckRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select status from enterprise where id = ? and uuid = ?  and status = '认证'";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getId());
        ps.setString(2, req.getUuid());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() == 0) {
          resp.put("content", false);
        } else {
          resp.put("content", true);
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BizReply reply = BizReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void update(EmployerUpdateRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "update enterprise set yingyezhizhao = ?, faren= ?, zhuceriqi= ?, zhuziguimo= ?, "
          + "yuangongshuliang= ?, yingyezhizhao_tu= ?, phone=?, address1= ?, address2= ?, address3= ?, "+
          " address4= ?, industry= ?, intro= ?, url= ? where id=? and uuid=?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getYingyezhizhao());
        ps.setString(2, req.getFaren());
        ps.setString(3, req.getZhuceriqi());
        ps.setString(4, req.getZhuziguimo());
        ps.setString(5, req.getYuangongshuliang());
        ps.setString(6, req.getYingyezhizhaoTu());
        ps.setString(7, req.getPhone());
        ps.setString(8, req.getAddress1());
        ps.setString(9, req.getAddress2());
        ps.setString(10, req.getAddress3());
        ps.setString(11, req.getAddress4());
        ps.setString(12, req.getIndustry());
        ps.setString(13, req.getIntro());
        ps.setString(14, req.getUrl());
        ps.setInt(15, req.getId());
        ps.setString(16, req.getUuid());
        ps.execute();
        resp.put("content", true);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BizReply reply = BizReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void subject(EmployerSubjectRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    String sql = "select id,uuid,name from enterprise where subject = ?";
    try (Connection conn = Persistence.getConn()) {
      try(PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getName());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BizReply reply = BizReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void jobFairList(EmployerJobFairListRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql =
      "select id, uuid, status, name, yingyezhizhao, phone, "+
      "faren, zhuceriqi, zhuziguimo, yuangongshuliang, address1, "+
      "address2, address3, address4, industry, intro, url, date, subject "+
      "from enterprise "+
      "where id in (select enterprise_id from recruitment" +
      "             where json_search(job_fair_id, \"one\", ?))";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getJobFairId());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BizReply reply = BizReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
