package billboard.service;

import com.google.gson.Gson;
import io.grpc.stub.StreamObserver;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class DeliveryServiceImpl extends DeliveryGrpc.DeliveryImplBase {
  private static final Logger logger = LoggerFactory.getLogger(DeliveryServiceImpl.class);

  @Override
  public void get(DeliveryProto.GetRequest req, StreamObserver<DeliveryProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from delivery where id = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getId());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() == 0) {
          resp.put("content", false);
        } else {
          resp.put("content", result.get(0));
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    DeliveryProto.Reply reply = DeliveryProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  /**
   * to-do: 分离各表数据
   * @param req
   * @param responseObserver
   */
  @Override
  public void recruitmentList(DeliveryProto.RecruitmentListRequest req, StreamObserver<DeliveryProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select d.status,r.education, r.uuid, "
      +"r.name as name, r.school,d.datime, d.id, d.recruitment_id, d.resume_id "
      +"from delivery d left join resume r on d.resume_id = r.id" +
      " where recruitment_id = ? and recruitment_uuid = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getRecruitmentId());
        ps.setString(2, req.getRecruitmentUuid());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    DeliveryProto.Reply reply = DeliveryProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  /**
   * to-do: 分离各表数据
   * @param req
   * @param responseObserver
   */
  @Override
  public void details(DeliveryProto.DetailsRequest req, StreamObserver<DeliveryProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn();) {
      String sql = "select d.*,r.name as recruitment_name,re.common_user_id,re.name,"
          + "  re.phone,re.email,re.gender, re.birthday,re.school,re.education,"
          + "  re.date_begin,re.date_end,re.major,re.qiwangzhiwei,re.qiwanghangye,"
          + "  re.address1,re.address2,re.address3,re.yixiangchengshi,re.ziwopingjia \n" + "from delivery d\n"
          + "  left join recruitment r on d.recruitment_id = r.id\n" + "  left join resume re on d.resume_id = re.id\n"
          + " where d.id = ? and re.uuid = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getId());
        ps.setString(2, req.getUuid());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() == 0) {
          resp.put("content", false);
        } else {
          resp.put("content", result.get(0));
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    DeliveryProto.Reply reply = DeliveryProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  /**
   * to-do: 分离各表数据
   * @param req
   * @param responseObserver
   */
  @Override
  public void userDeliveryList(DeliveryProto.UserDeliveryListRequest req, StreamObserver<DeliveryProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select r.*, d.status, d.datime from delivery d  join "
          + "    recruitment r on d.recruitment_id = r.id  where "
          + "     (select re.id from resume re where re.common_user_id= ? limit 1) = d.resume_id";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getCommonUserId());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    DeliveryProto.Reply reply = DeliveryProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void userDelivery(DeliveryProto.UserDeliveryRequest req, StreamObserver<DeliveryProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from delivery"
          + " where resume_id = (select id from resume where common_user_id = ? limit 1 )" + " and recruitment_id=?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getCommonUserId());
        ps.setInt(2, req.getRecruitmentId());
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        if (result.size() == 0) {
          resp.put("content", false);
        } else {
          resp.put("content", result.get(0));
        }
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    DeliveryProto.Reply reply = DeliveryProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void insert(DeliveryProto.InsertRequest req, StreamObserver<DeliveryProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "insert into delivery (resume_id, resume_uuid, recruitment_id, recruitment_uuid, datime)"
          + " value ( (select id from resume where common_user_id = ? limit 1),"
          + "(select uuid from resume where common_user_id = ? limit 1),?,?,?)";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getCommonUserId());
        ps.setInt(2, req.getCommonUserId());
        ps.setInt(3, req.getRecruitmentId());
        ps.setString(4, req.getRecruitmentUuid());
        ps.setString(5, req.getDatime());
        boolean rs = ps.execute();
        resp.put("content", rs);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    DeliveryProto.Reply reply = DeliveryProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void search(DeliveryProto.SearchRequest req, StreamObserver<DeliveryProto.Reply> responseObserver) {
    String resp = "[]";
    try (Connection cnx = Persistence.getConn()) {
      String sql = """
          select re.name as recruitment_name
            , re.industry
            , d.status
            , r.education
            , r.uuid
            , r.name as name
            , r.school
            , d.datime
            , d.id
            , d.recruitment_id
            , d.resume_id
          from delivery d
            left join resume r on d.resume_id = r.id
            left join recruitment re on d.recruitment_id = re.id
          where re.enterprise_id = ?
            and re.enterprise_uuid = ?
            and position(? in r.name) > 0
            and position(? in re.name) > 0
            and position(? in d.datime) > 0
            and position(? in d.status) > 0
            and position(? in r.education) > 0
          order by d.datime desc
          """;
      List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler(),
          req.getId(),
          req.getUuid(),
          req.getName(),
          req.getRecruitmentName(),
          req.getDate(),
          req.getStatus(),
          req.getEducation());
      resp = new Gson().toJson(result);
    } catch (Exception e) {
      logger.error("", e);
    }
    DeliveryProto.Reply reply = DeliveryProto.Reply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
    // Gson gson = new Gson();
    // Map<String, Object> resp = new HashMap<>();
    // resp.put("message", "");
    // resp.put("content", "");
    // try (Connection conn = Persistence.getConn()) {
    //   String sql = "select re.name as recruitment_name, re.industry, d.status,"
    //       + "r.education, r.uuid, r.name as name, r.school,d.datime,  d.id, d.recruitment_id, d.resume_id "
    //       + "from delivery d left join resume r on d.resume_id = r.id left join recruitment re on d.recruitment_id = re.id "
    //       + "where re.enterprise_id = ? and  re.enterprise_uuid = ? ";
    //   List<String> list = new ArrayList<>();
    //   list.add(String.valueOf(req.getId()));
    //   list.add(req.getUuid());
    //   if (req.getName() != null && !"".equals(req.getName())) {
    //     list.add(req.getName());
    //     sql += " and r.name like CONCAT(?,'%') ";
    //   }

    //   if (req.getRecruitmentName() != null && !"".equals(req.getRecruitmentName())) {
    //     list.add(req.getRecruitmentName());
    //     sql += " and re.name like CONCAT(?,'%') ";
    //   }

    //   if (req.getDate() != null && !"".equals(req.getDate())) {
    //     list.add(req.getDate());
    //     sql += " and SUBSTRING_INDEX(d.datime,' ',1) = ? ";
    //   }

    //   if (req.getStatus() != null && !"".equals(req.getStatus() )) {
    //     list.add(req.getStatus());
    //     sql += " and d.status = ? ";
    //   }

    //   if (req.getEducation() != null && !"".equals(req.getEducation())) {
    //     list.add(req.getEducation());
    //     sql += " and re.name = ? ";
    //   }
    //   sql += " ORDER BY d.datime DESC";
    //   try (PreparedStatement ps = conn.prepareStatement(sql)) {
    //     for (int inx = 0; inx < list.size(); inx++) {
    //       ps.setString(inx + 1, list.get(inx));
    //     }
    //     ResultSet rs = ps.executeQuery();
    //     List<Map<String, Object>> result = Persistence.getList(rs);
    //     resp.put("content", result);
    //   }
    // } catch (Exception e) {
    //   logger.error("", e);
    //   resp.put("message", "gRPC服务器错误");
    // }
    // DeliveryProto.Reply reply = DeliveryProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    // responseObserver.onNext(reply);
    // responseObserver.onCompleted();
  }

  @Override
  public void status(DeliveryProto.StatusRequest req, StreamObserver<DeliveryProto.Reply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "update delivery set status = ? where id = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getStatus());
        ps.setInt(2, req.getId());
        ps.execute();
        resp.put("content", true);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    DeliveryProto.Reply reply = DeliveryProto.Reply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
