package billboard.service;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
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
  public void get(Resume2102GetRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "{}";
    try (Connection cnx = Persistence.getConn()) {
      if ("".equals(req.getOption())) {
        String sql = "select * from resume where id = ? and uuid = ?";
        Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler(),
            Integer.parseInt(req.getParamMap().get("id")),
            req.getParamMap().get("uuid"));
        resp = new Gson().toJson(result);
      } else if ("by-user".equals(req.getOption())) {
        String sql = """
            select *
            from resume
            where common_user_id = ?
              and (select uuid from common_user where id = common_user_id) = ?
            """;
        Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler(),
            Integer.parseInt(req.getParamMap().get("id")),
            req.getParamMap().get("uuid"));
        resp = new Gson().toJson(result);
      } else if ("valid".equals(req.getOption())) {
        String sql = """
            select name, phone, email, gender, birthday
            from resume
            where common_user_id = ?
            """;
        Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler(),
            Integer.parseInt(req.getParamMap().get("id")));
        if ("".equals(result.get("name")) || "".equals(result.get("phone")) ||
            "".equals(result.get("email")) || "".equals(result.get("gender")) ||
            "".equals(result.get("birthday"))) {
          resp = "false";
        } else {
          resp = "true";
        }
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
      if ("".equals(req.getOption())) {
        String sql = """
            update resume
            set name = ?,
              phone = ?,
              email = ?,
              gender = ?,
              birthday = ?,
              school = ?,
              education = ?,
              date_begin = ?,
              date_end = ?,
              major = ?,
              qiwangzhiwei = ?,
              qiwanghangye = ?,
              address1 = ?,
              address2 = ?,
              address3 = ?,
              yixiangchengshi = ?,
              ziwopingjia = ?,
              career = ?,
              record = ?
            where common_user_id = ?
              and uuid = ?
            """;
        new QueryRunner().execute(cnx, sql,
            req.getParamMap().get("name"),
            req.getParamMap().get("phone"),
            req.getParamMap().get("email"),
            req.getParamMap().get("gender"),
            req.getParamMap().get("birthday"),
            req.getParamMap().get("school"),
            req.getParamMap().get("education"),
            req.getParamMap().get("date_begin"),
            req.getParamMap().get("date_end"),
            req.getParamMap().get("major"),
            req.getParamMap().get("qiwangzhiwei"),
            req.getParamMap().get("qiwanghangye"),
            req.getParamMap().get("address1"),
            req.getParamMap().get("address2"),
            req.getParamMap().get("address3"),
            req.getParamMap().get("yixiangchengshi"),
            req.getParamMap().get("ziwopingjia"),
            req.getParamMap().get("career"),
            req.getParamMap().get("record"),
            req.getParamMap().get("common_user_id"),
            req.getParamMap().get("uuid"));
      } else if ("refresh".equals(req.getOption())) {
        String sql = """
            update resume set date_update = now() where common_user_id = ?
            """;
        new QueryRunner().execute(cnx, sql,
            Integer.parseInt(req.getParamMap().get("candidate_id")));
      } else if ("status".equals(req.getOption())) {
        String sql = "update resume set status = ? where common_user_id = ? and uuid = ?";
        new QueryRunner().execute(cnx, sql,
            req.getParamMap().get("status"),
            Integer.parseInt(req.getParamMap().get("candidate_id")),
            req.getParamMap().get("uuid"));
      } else if ("save-career".equals(req.getOption())) {
        String sql = """
            update resume
            set career = json_array_append(career, '$', ?)
            where common_user_id = ?
            """;
        new QueryRunner().execute(cnx, sql,
            req.getParamMap().get("data"),
            Integer.parseInt(req.getParamMap().get("candidate_id")));
      } else if ("update-career".equals(req.getOption())) {
        String sql = """
            update resume
            set career = ?
            where common_user_id = ?
            """;
        new QueryRunner().execute(cnx, sql,
            req.getParamMap().get("data"),
            Integer.parseInt(req.getParamMap().get("candidate_id")));
      } else if ("save-record".equals(req.getOption())) {
        String sql = """
            update resume
            set record = json_array_append(record, '$', ?)
            where common_user_id = ?
            """;
        new QueryRunner().execute(cnx, sql,
            req.getParamMap().get("data"),
            Integer.parseInt(req.getParamMap().get("candidate_id")));
      } else if ("save-record".equals(req.getOption())) {
        String sql = """
            update resume
            set record = ?
            where common_user_id = ?
            """;
        new QueryRunner().execute(cnx, sql,
            req.getParamMap().get("data"),
            Integer.parseInt(req.getParamMap().get("candidate_id")));
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    responseObserver.onNext(null);
    responseObserver.onCompleted();
  }
}
