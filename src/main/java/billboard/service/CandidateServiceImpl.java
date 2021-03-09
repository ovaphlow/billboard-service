package billboard.service;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

public class CandidateServiceImpl extends CandidateGrpc.CandidateImplBase {
  private static final Logger logger = LoggerFactory.getLogger(CandidateServiceImpl.class);

  @Override
  public void filter(CandidateFilterRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "[]";
    try (Connection cnx = Persistence.getConn()) {
      if ("hypervisor".equals(req.getOption())) {
        String sql = """
            select id, uuid, name, phone, email
            from common_user
            where position(? in name) > 0
              or position(? in phone) > 0
            order by id desc
            limit 100
            """;
        List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler(),
            req.getDataMap().get("keyword"),
            req.getDataMap().get("keyword"));
        resp = new Gson().toJson(result);
      } else if ("by-id-list".equals(req.getOption())) {
        String sql = """
            select id, uuid, name, phone, email
            from common_user
            where id in (%s)""";
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
  public void get(CandidateGetRequest req, StreamObserver<BizReply> responseObserver) {
    String resp = "{}";
    try (Connection cnx = Persistence.getConn()) {
      String sql = """
          select id, uuid, name, phone, email
          from common_user
          where id = ?
            and uuid = ?
          """;
      Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler(),
          req.getId(),
          req.getUuid());
      resp = new Gson().toJson(result);
    } catch (Exception e) {
      logger.error("", e);
    }
    BizReply reply = BizReply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void statistic(CandidateStatisticRequest req,
      StreamObserver<BizReply> responseObserver) {
    String resp = "{}";
    try (Connection cnx = Persistence.getConn()) {
      if ("hypervisor-all".equals(req.getOption())) {
        String sql = "select count(*) as qty from common_user";
        Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler());
        resp = new Gson().toJson(result);
      } else if ("hypervisor-today".equals(req.getOption())) {
        String sql = """
            select count(*) as qty
            from common_user
            where position(? in date_create) > 0
            """;
        Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler(),
            req.getDataMap().get("date"));
        resp = new Gson().toJson(result);
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    BizReply reply = BizReply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
