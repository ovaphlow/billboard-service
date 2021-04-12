package billboard.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.gson.Gson;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobServiceImpl extends JobGrpc.JobImplBase {
  private static final Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);

  @Override
  public void statistic(JobStatisticRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "{}";
    try (Connection cnx = Persistence.getConn()) {
      if ("hypervisor-all".equals(req.getOption())) {
        String sql = "select count(*) as qty from recruitment";
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

  /**
   * 2020-11-10
   * data list
   * 用于之后的接口整合，候选代码。
   * param { category: String, filter: Map }
   * return filter results toJson(List<Map<String, Object>>)
   */
  @Override
  public void filter(JobFilterRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "[]";
    try (Connection cnx = Persistence.getConn()) {
      if ("".equals(req.getCategory())) {
        String sql = "select * from recruitment where status = '在招' order by id desc limit 100";
        PreparedStatement ps = cnx.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp = new Gson().toJson(result);
      } else if ("wx-default-list".equals(req.getCategory())) {
        int page = Integer.parseInt(req.getParamMap().get("page"));
        int offset = page > 1 ? (page - 1) * 100 : 0;
        String sql = """
            select *
            from recruitment
            where status = '在招'
              and position(category in ?) > 0
              and position(? in address2) > 0
              and position(? in industry) > 0
              and (
                position(? in name) > 0
                or enterprise_id in (select id from enterprise where position(? in name) > 0)
              )
            order by date_refresh desc, id desc
            limit ?, 100
            """;
        List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler(),
            req.getParamMap().get("category"),
            req.getParamMap().get("city"),
            req.getParamMap().get("industry"),
            req.getParamMap().get("keyword"),
            req.getParamMap().get("keyword"),
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
  public void save(JobSaveRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String uuid = UUID.randomUUID().toString();
      String sql = """
          insert into recruitment (
            enterprise_id,
            enterprise_uuid,
            name,
            qty,
            description,
            requirement,
            address1,
            address2,
            address3,
            date,
            salary1,
            salary2,
            education,
            category,
            industry,
            position,
            uuid,
            job_fair_id
          )
          values (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            convert('[]', json)
          )
          """;
      new QueryRunner().execute(conn, sql,
          req.getEnterpriseId(),
          req.getEnterpriseUuid(),
          req.getName(),
          req.getQty(),
          req.getDescription(),
          req.getRequirement(),
          req.getAddress1(),
          req.getAddress2(),
          req.getAddress3(),
          new SimpleDateFormat("yyyy-MM-dd").format(new Date()),
          req.getSalary1(),
          req.getSalary2(),
          req.getEducation(),
          req.getCategory(),
          req.getIndustry(),
          req.getPosition(),
          uuid);
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BizReply reply = BizReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void get(JobGetRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = """
          select r.*, e.name as enterprise_name, e.uuid as enterprise_uuid, u.id as ent_user_id
          from recruitment r left join enterprise e on e.id=r.enterprise_id
          left join enterprise_user u on u.enterprise_id = e.id
          where r.id = ? and r.uuid = ?
          """;
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
    BizReply reply = BizReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void update(JobUpdateRequest req, StreamObserver<Empty> responseObserver) {
    try (Connection cnx = Persistence.getConn()) {
      if ("".equals(req.getOption())) {
        String sql = """
            update recruitment
            set name = ?,
              qty = ?,
              description = ?,
              requirement = ?,
              address1 = ?,
              address2 = ?,
              address3 = ?,
              salary1 = ?,
              salary2 = ?,
              education = ?,
              category = ?,
              industry = ?,
              position = ?
            where id = ?
              and uuid = ?
            """;
        new QueryRunner().execute(cnx, sql,
            req.getDataMap().get("name"),
            req.getDataMap().get("qty"),
            req.getDataMap().get("description"),
            req.getDataMap().get("requirement"),
            req.getDataMap().get("address1"),
            req.getDataMap().get("address2"),
            req.getDataMap().get("address3"),
            req.getDataMap().get("salary1"),
            req.getDataMap().get("salary2"),
            req.getDataMap().get("education"),
            req.getDataMap().get("category"),
            req.getDataMap().get("industry"),
            req.getDataMap().get("position"),
            Integer.parseInt(req.getDataMap().get("id")),
            req.getDataMap().get("uuid"));
      } else if ("status".equals(req.getOption())) {
        String sql = """
            update recruitment
            set status = ?
            where id = ?
              and uuid = ?
            """;
        new QueryRunner().execute(cnx, sql,
            req.getDataMap().get("status"),
            Integer.parseInt(req.getDataMap().get("id")),
            req.getDataMap().get("uuid"));
      } else if ("refresh".equals(req.getOption())) {
        String sql = """
            update recruitment set date_refresh = now() where id = ?
            """;
        new QueryRunner().execute(cnx, sql, Integer.parseInt(req.getDataMap().get("id")));
      } else if ("save-fair-by-employer".equals(req.getOption())) {
        String sql = """
            update recruitment
            set job_fair_id = json_array_append(job_fair_id, '$', ?)
            where enterprise_id = ?
              and id in (%s)
            """;
        sql = String.format(sql, req.getDataMap().get("list"));
        new QueryRunner().execute(cnx, sql,
            req.getDataMap().get("fair_id"),
            req.getDataMap().get("employer_id"));
      } else if ("remove-fair-by-employer".equals(req.getOption())) {
        String sql = """
            update recruitment
            set job_fair_id = json_remove(job_fair_id,
                json_unquote(json_search(job_fair_id, 'one', ?)))
            where enterprise_id = ?
              and id in (%s)
            """;
        sql = String.format(sql, req.getDataMap().get("list"));
        new QueryRunner().execute(cnx, sql,
            req.getDataMap().get("fair_id"),
            req.getDataMap().get("employer_id"));
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    responseObserver.onNext(null);
    responseObserver.onCompleted();
  }

  @Override
  public void enterpriseList(JobEnterpriseListRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * " +
          "from recruitment " +
          "where enterprise_id = ? " +
          "and (select uuid from enterprise where id = enterprise_id ) = ? " +
          "and status = '在招' " +
          "ORDER BY date DESC " +
          "limit 200";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getId());
        ps.setString(2, req.getUuid());
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
  public void enterpriseSearch(JobEnterpriseSearchRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "[]";
    try (Connection cnx = Persistence.getConn()) {
      String sql = """
          select id
            , uuid
            , enterprise_id
            , enterprise_uuid
            , name
            , qty
            , address1
            , address2
            , address3
            , date
            , salary1
            , salary2
            , education
            , category
            , status
            , industry
            , position
            , job_fair_id
            , (
              select count(*)
              from browse_journal
              where data_id = recruitment.id
                and data_uuid = recruitment.uuid
            ) as journal
            , (
              select count(*)
              from delivery
              where recruitment_id = recruitment.id
                and recruitment_uuid = recruitment.uuid
            ) as delivery
          from recruitment
          where enterprise_id = ?
            and enterprise_uuid = ?
            and position(? in name) > 0
            and position(? in category) > 0
            and position(? in date) > 0
            and position(? in status) > 0
            and position(? in education) > 0
          order by date desc
          """;
      List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler(),
          req.getEnterpriseId(),
          req.getUuid(),
          req.getName(),
          req.getCategory(),
          req.getDate(),
          req.getStatus(),
          req.getEducation());
      resp = new Gson().toJson(result);
    } catch (Exception e) {
      logger.error("", e);
    }
    BizReply reply = BizReply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
    // Gson gson = new Gson();
    // Map<String, Object> resp = new HashMap<>();
    // resp.put("message", "");
    // resp.put("content", "");
    // try (Connection conn = Persistence.getConn()) {
    //   String sql = "select id, enterprise_id, enterprise_uuid, name, qty, address1, address2, address3, " +
    //       "date, salary1, salary2, education, category, status, industry, position, uuid, job_fair_id, " +
    //       "(select count(*) from browse_journal where data_id = recruitment.id and data_uuid = recruitment.uuid) as journal, " +
    //       "(select count(*) from delivery where recruitment_id = recruitment.id and recruitment_uuid = recruitment.uuid) as delivery " +
    //       "from recruitment " +
    //       "where enterprise_id = ? and enterprise_uuid = ?";
    //   List<String> list = new ArrayList<>();
    //   list.add(req.getEnterpriseId());
    //   list.add(req.getUuid());
    //   if (req.getName() != null && !"".equals(req.getName())) {
    //     sql += " and name = ? ";
    //     list.add(req.getName());
    //   }
    //   if (req.getCategory() != null && !"".equals(req.getCategory())) {
    //     sql += " and category = ? ";
    //     list.add(req.getCategory());
    //   }
    //   if (req.getDate() != null && !"".equals(req.getDate())) {
    //     sql += " and date = ? ";
    //     list.add(req.getDate());
    //   }
    //   if (req.getStatus() != null && !"".equals(req.getStatus())) {
    //     sql += " and status = ? ";
    //     list.add(req.getStatus());
    //   }
    //   if (req.getEducation() != null && !"".equals(req.getEducation())) {
    //     sql += " and education = ? ";
    //     list.add(req.getEducation());
    //   }
    //   sql += " ORDER BY date DESC";
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
    // BizReply reply = BizReply.newBuilder().setData(gson.toJson(resp)).build();
    // responseObserver.onNext(reply);
    // responseObserver.onCompleted();
  }

  @Override
  public void subject(JobSubjectRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * " +
          "from recruitment " +
          "where enterprise_id in (select id from enterprise where subject = ?) " +
          "ORDER BY date DESC " +
          "limit 200";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, req.getSubject());
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
  public void jobFairEntList(JobFairEntListRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql =
          "select * from recruitment " +
              "where json_search(job_fair_id, \"one\", ?) " +
              "      and enterprise_id=? and enterprise_uuid= ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, req.getJobFairId());
        ps.setInt(2, req.getEntId());
        ps.setString(3, req.getEntUuid());
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
  public void jobFairList(JobFairListRequest req, StreamObserver<BizReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from recruitment where json_search(job_fair_id, \"one\", ?)";
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
