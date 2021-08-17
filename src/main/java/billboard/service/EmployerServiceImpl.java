package billboard.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

  @Override
  public void signUp(EmployerSignUpRequest req,
      StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      Map<String, String> err = new HashMap<>();
      List<Map<String, Object>> result = new ArrayList<>();
      int enterprise_id = 0;
      String sql = "select * from captcha where user_category='企业用户' and code=? and email=? "
          + "and str_to_date(datime,'%Y-%m-%d %H:%i:%s') >= now()-interval 10 minute ORDER BY datime DESC limit 1";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getCode());
        ps.setString(2, req.getEmail());
        ResultSet rs = ps.executeQuery();
        result = Persistence.getList(rs);
        if (result.size() == 0) {
          err.put("code", "0");
        }
      }
      if (err.keySet().size() != 0) {
        resp.put("message", err);
      } else {
        sql = "select (select count(*) from enterprise_user where email = ? ) as email,"
            + "(select count(*) from enterprise where name = ? ) as ent_name";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, req.getEmail());
          ps.setString(2, req.getEntName());
          ResultSet rs = ps.executeQuery();
          result = Persistence.getList(rs);
          result.get(0).forEach((k, v) -> {
            if (!"0".equals(v.toString())) {
              err.put(k, v.toString());
            }
          });
        }
        if (err.keySet().size() != 0) {
          resp.put("message", err);
        } else {
          String entUUID = UUID.randomUUID().toString();
          String entUserUUID = UUID.randomUUID().toString();
          sql = "insert into enterprise (uuid,name,yingyezhizhao_tu,date) value (?,?,'',current_date)";
          try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, entUUID);
            ps.setString(2, req.getEntName());
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
              enterprise_id = rs.getInt(1);
            }
          }
          sql = "insert into enterprise_user (uuid, enterprise_uuid ,enterprise_id, password, name, email, salt) value (?,?,?,?,?,?,?)";
          try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, entUserUUID);
            ps.setString(2, entUUID);
            ps.setInt(3, enterprise_id);
            ps.setString(4, req.getPassword());
            ps.setString(5, req.getEntName());
            ps.setString(6, req.getEmail());
            ps.setString(7, req.getSalt());
            ps.executeUpdate();
          }
          sql = "delete from captcha where user_category='企业用户' and code=? and email=? ";
          try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, req.getCode());
            ps.setString(2, req.getEmail());
            ps.execute();
          }
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
  public void signIn(EmployerSignInRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      List<Map<String, Object>> result = new ArrayList<>();
      String sql = "select * from enterprise_user where (phone = ? or email = ?)";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getPhoneEmail());
        ps.setString(2, req.getPhoneEmail());
        ResultSet rs = ps.executeQuery();
        result = Persistence.getList(rs);
      }
      if (result.size() == 0) {
        resp.put("message", "账号或密码错误");
      } else {
        Map<String, Object> userData = result.get(0);
        sql = "insert into login_journal (user_id, ip, address, category, datime) value (?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, userData.get("id").toString());
          ps.setString(2, req.getIp());
          ps.setString(3, req.getAddress());
          ps.setString(4, "企业用户");
          ps.setString(5, new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date()));
          ps.execute();
          resp.put("content", userData);
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
  public void updateUser(EmployerUpdateUserRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      List<Map<String, Object>> result = new ArrayList<>();
      Map<String, Object> err = new HashMap<>();
      String sql = "select * from captcha where user_category='企业用户' and code=? and email=? "
          + "and str_to_date(datime,'%Y-%m-%d %H:%i:%s') >= now()-interval 10 minute ORDER BY datime DESC limit 1";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getCode());
        ps.setString(2, req.getEmail());
        ResultSet rs = ps.executeQuery();
        result = Persistence.getList(rs);
        if (result.size() == 0) {
          err.put("code", "0");
        }
      }
      if (err.keySet().size() != 0) {
        resp.put("message", err);
      } else {
        sql = "update enterprise_user set email = ? , phone = ? where id = ? and uuid = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, req.getEmail());
          ps.setString(2, req.getPhone());
          ps.setInt(3, req.getId());
          ps.setString(4, req.getUuid());
          ps.execute();
        }
        sql = "delete from captcha where user_category='企业用户' and code=? and email=? ";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, req.getCode());
          ps.setString(2, req.getEmail());
          ps.execute();
        }
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
  public void updatePassword(EmployerUpdatePasswordRequest req,
      StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "update enterprise_user set password = ? , salt = ? where id = ? and uuid = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getPassword());
        ps.setString(2, req.getSalt());
        ps.setInt(3, req.getId());
        ps.setString(4, req.getUuid());
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
  public void upPasswordCheck(EmployerUpPasswordCheckRequest req,
      StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      List<Map<String, Object>> result;
      String sql = "select * from enterprise_user where id = ? and uuid = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getId());
        ps.setString(2, req.getUuid());
        ResultSet rs = ps.executeQuery();
        result = Persistence.getList(rs);
      }
      Map<String, String> err = new HashMap<>();
      if (result.size() == 0) {
        err.put("old_password", "0");
        resp.put("message", err);
      } else {
        resp.put("content", result.get(0));
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
  public void recover(EmployerRecoverRequest req,
      StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      List<Map<String, Object>> result = new ArrayList<>();
      String sql = """
          select *
          from captcha
          where user_category = ?
            and code = ?
            and email = ?
            and str_to_date(datime, '%Y-%m-%d %H:%i:%s') >= now() - interval 20 minute
          ORDER BY datime DESC
          limit 1
          """;
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getUserCategory());
        ps.setString(2, req.getCode());
        ps.setString(3, req.getEmail());
        ResultSet rs = ps.executeQuery();
        result = Persistence.getList(rs);
      }
      Map<String, String> err = new HashMap<>();
      if (result.size() == 0) {
        err.put("code", "0");
      } else {
        sql = "select * from enterprise_user where email = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, req.getEmail());
          ResultSet rs = ps.executeQuery();
          result = Persistence.getList(rs);
        }
        sql = "delete from captcha where user_category=? and code=? and email=? ";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, req.getUserCategory());
          ps.setString(2, req.getCode());
          ps.setString(3, req.getEmail());
          ps.execute();
        }
        resp.put("content", result.get(0));
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
  public void checkEmail(EmployerCheckEmailRequest req,
      StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from enterprise_user where email = ? and id != ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getEmail());
        ps.setInt(2, req.getId());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() != 0) {
          resp.put("message", "该邮箱已被使用!");
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
  public void checkPhone(EmployerCheckPhoneRequest req,
      StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from enterprise_user where phone = ? and id != ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getPhone());
        ps.setInt(2, req.getId());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() != 0) {
          resp.put("message", "该电话号码已被使用!");
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
  public void checkRecover(EmployerCheckRecoverRequest req,
      StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from enterprise_user where email = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getEmail());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() == 0) {
          resp.put("message", "该邮箱不存在!");
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
}
