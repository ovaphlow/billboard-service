package billboard.service;

import com.google.gson.Gson;
import io.grpc.stub.StreamObserver;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

public class CommonUserServiceImpl extends CommonUserGrpc.CommonUserImplBase {
  private static final Logger logger = LoggerFactory.getLogger(CommonUserServiceImpl.class);

  @Override
  public void review(CommonUserProto.ReviewRequest req, StreamObserver<CommonUserProto.Reply> responseObserver) {
    String resp = "";
    try (Connection cnx = Persistence.getConn()) {
      if ("".equals(req.getOption())) {
        String sql = """
            select wx_openid
            from common_user
            where id = ?
              and uuid = ?
            limit 1
            """;
        QueryRunner queryRunner = new QueryRunner();
        Map<String, Object> result = queryRunner.query(cnx, sql, new MapHandler(),
            req.getId(),
            req.getUuid());
        if (!req.getDataMap().get("wx_openid").equals(result.get("wx_openid").toString())) {
          sql = """
              update common_user
              set wx_openid = ?
              where id = ?
                and uuid = ?
              """;
          queryRunner.update(cnx, sql, req.getDataMap().get("wx_openid"), req.getId(), req.getUuid());
        }
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void get(CommonUserProto.GetRequest req, StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select phone,name,id,uuid,email from common_user where id = ? and uuid = ? ";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getId());
        ps.setString(2, req.getUuid());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result.get(0));
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void signIn(CommonUserProto.SignInRequest req, StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      Map<String, String> err = new HashMap<>();
      String sql = """
          select *
          from captcha
          where user_category='个人用户'
            and code=?
            and email=?
            and str_to_date(datime,'%Y-%m-%d %H:%i:%s') >= now()-interval 10 minute
          order by datime desc
          limit 1
          """;
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getCode());
        ps.setString(2, req.getEmail());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() == 0) {
          err.put("code", "0");
        }
      }
      if (err.keySet().size() != 0) {
        resp.put("message", err);
      } else {
        sql = "select " + "(select count(*) from common_user where email = ? ) as email,"
            + "(select count(*) from common_user where name = ? ) as name";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, req.getEmail());
          ps.setString(2, req.getName());
          ResultSet rs = ps.executeQuery();
          List<Map<String, Object>> result = Persistence.getList(rs);
          result.get(0).forEach((k, v) -> {
            if (!"0".equals(v.toString())) {
              err.put(k, v.toString());
            }
          });
        }
        if (err.keySet().size() != 0) {
          resp.put("message", err);
        } else {
          sql = """
              insert into common_user
                (uuid,email,password,name,salt)
              value
                (uuid(), ?, ?, ?, ?)
              """;
          try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, req.getEmail());
            ps.setString(2, req.getPassword());
            ps.setString(3, req.getName());
            ps.setString(4, req.getSalt());
            ps.execute();
            resp.put("content", true);
          }
          sql = "delete from captcha where user_category='个人用户' and code=? and email=? ";
          try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, req.getCode());
            ps.setString(2, req.getEmail());
            ps.execute();
          }
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  /**
   * 登录
   * 2021-08 需要将微信的openid保存到数据库中（？）
   *
   * @param req
   * @param responseObserver
   */
  @Override
  public void logIn(CommonUserProto.LogInRequest req, StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = """
          select wx_openid, phone,name,id,uuid,email,salt,password
          from common_user
          where (phone = ? or email = ?)
          """;
      List<Map<String, Object>> result = new ArrayList<>();
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
          ps.setString(4, "个人用户");
          ps.setString(5, new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date()));
          ps.execute();
          resp.put("content", userData);
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void update(CommonUserProto.UpdateRequest req, StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      Map<String, String> err = new HashMap<>();
      String sql = "select * from captcha where user_category=? and code=? and email=? "
          + "and str_to_date(datime,'%Y-%m-%d %H:%i:%s') >= now()-interval 10 minute ORDER BY datime DESC limit 1";
      List<Map<String, Object>> result = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getUserCategory());
        ps.setString(2, req.getCode());
        ps.setString(3, req.getEmail());
        ResultSet rs = ps.executeQuery();
        result = Persistence.getList(rs);
      }
      if (result.size() == 0) {
        resp.put("message", "验证码错误!");
      } else {
        sql = "select (select count(*) from common_user where email = ? and id !=? ) as email,"
            + "(select count(*) from common_user where name = ? and id !=? ) as name,"
            + "(select count(*) from common_user where phone = ? and id !=? ) as phone";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, req.getEmail());
          ps.setInt(2, req.getId());
          ps.setString(3, req.getName());
          ps.setInt(4, req.getId());
          ps.setString(5, req.getPhone());
          ps.setInt(6, req.getId());
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
          sql = "update common_user set name = ?, phone=?, email=?  where id=?";
          try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, req.getName());
            ps.setString(2, req.getPhone());
            ps.setString(3, req.getEmail());
            ps.setInt(4, req.getId());
            ps.execute();
            resp.put("content", true);
          }
          sql = "delete from captcha where user_category=? and code=? and email=? ";
          try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, req.getUserCategory());
            ps.setString(2, req.getCode());
            ps.setString(3, req.getEmail());
            ps.execute();
          }
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void phone(CommonUserProto.PhoneRequest req, StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      List<Map<String, Object>> result = new ArrayList<>();
      String sql = "select * from common_user where phone=?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getPhone());
        ResultSet rs = ps.executeQuery();
        result = Persistence.getList(rs);
      }
      if (result.size() != 0) {
        resp.put("message", "该电话号已使用");
      } else {
        sql = "update common_user set phone=? where id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, req.getPhone());
          ps.setInt(2, req.getId());
          ps.execute();
          resp.put("content", true);
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void journal(CommonUserProto.JournalRequest req, StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from  login_journal where user_id = ? and category = ? ORDER BY datime DESC ";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getUserId());
        ps.setString(2, req.getCategory());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void recover(CommonUserProto.RecoverRequest req, StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from captcha where user_category=? and code=? and email=? "
          + "and str_to_date(datime,'%Y-%m-%d %H:%i:%s') >= now()-interval 10 minute ORDER BY datime DESC limit 1";
      Map<String, String> err = new HashMap<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getUserCategory());
        ps.setString(2, req.getCode());
        ps.setString(3, req.getEmail());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() == 0) {
          err.put("code", "0");
        }
      }
      if (err.keySet().size() != 0) {
        resp.put("message", err);
      } else {
        sql = "update common_user set password =?, salt = ?  where email= ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, req.getPassword());
          ps.setString(2, req.getSalt());
          ps.setString(3, req.getEmail());
          ps.execute();
        }
        sql = "delete from captcha where user_category=? and code=? and email=? ";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, req.getUserCategory());
          ps.setString(2, req.getCode());
          ps.setString(3, req.getEmail());
          ps.execute();
        }
        resp.put("content", true);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void checkEmail(CommonUserProto.CheckEmailRequest req,
                         StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from common_user where email = ? and id != ?";
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
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void checkRecover(CommonUserProto.CheckRecoverRequest req,
                           StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from common_user where email = ?";
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
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void checkPassword(CommonUserProto.CheckPasswordRequest req,
                            StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select salt,password from common_user where id = ? and uuid = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getId());
        ps.setString(2, req.getUuid());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result.get(0));
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void checkCaptcha(CommonUserProto.CheckCaptchaRequest req,
                           StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from captcha where user_category=? and code=? and email= ? "
          + "and str_to_date(datime,'%Y-%m-%d %H:%i:%s') >= now()-interval 10 minute ORDER BY datime DESC limit 1";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, "个人用户");
        ps.setString(2, req.getCode());
        ps.setString(3, req.getEmail());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result.size() != 0);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void updatePassword(CommonUserProto.UpdatePasswordRequest req,
                             StreamObserver<CommonUserProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "update common_user set password = ?, salt = ? where id = ? and uuid = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getPassword());
        ps.setString(2, req.getSalt());
        ps.setInt(3, req.getId());
        ps.setString(4, req.getUuid());
        ps.execute();
        resp.put("content", true);
      }
      sql = "delete from captcha where user_category=? and email=? ";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, "个人用户");
        ps.setString(2, req.getEmail());
        ps.execute();
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CommonUserProto.Reply reply = CommonUserProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
