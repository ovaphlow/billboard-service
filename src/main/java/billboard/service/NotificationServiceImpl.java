package billboard.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import io.grpc.stub.StreamObserver;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationServiceImpl extends NotificationGrpc.NotificationImplBase {
  private static final Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);

  @Override
  public void statistic(NotificationStatisticRequest req,
      StreamObserver<BulletinReply> responseObserver) {
    String resp = "{}";
    try (Connection cnx = Persistence.getConn()) {
      if ("hypervisor-all".equals(req.getOption())) {
        String sql = "select count(*) as qty from recommend";
        Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler());
        resp = new Gson().toJson(result);
      } else if ("hypervisor-today".equals(req.getOption())) {
        String sql = """
            select count(*) as qty
            from recommend
            where position(? in date_create) > 0
            """;
        Map<String, Object> result = new QueryRunner().query(cnx, sql, new MapHandler(),
            req.getDataMap().get("date"));
        resp = new Gson().toJson(result);
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    BulletinReply reply = BulletinReply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  /**
   * 2021-01
   * 通用查询/列表
   * 用于之后的接口整合，候选代码
   */
  @Override
  public void filter(NotificationFilterRequest req, StreamObserver<BulletinReply> responseObserver) {
    String resp = "[]";
    try (Connection cnx = Persistence.getConn()) {
      if ("wx-default-list".equals(req.getFilter())) {
        String sql = """
            select id, uuid, category, title, date1, date2, address_level1, address_level2,
              publisher, qty, baomignfangshi
            from recommend
            where
              position(category in ?) > 0
              and position(? in address_level2) > 0
              and position(? in title) > 0
            order by date1 desc, date2
            limit ?, 50
            """;
        int page = Integer.parseInt(req.getParamMap().get("page"));
        int offset = page > 1 ? (page - 1) * 50 : 0;
        List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler(),
            req.getParamMap().get("category"),
            req.getParamMap().get("address_level2"),
            req.getParamMap().get("keyword"),
            offset);
        resp = new Gson().toJson(result);
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    BulletinReply reply = BulletinReply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  /**
   * 2021-01
   * to-do: remove procedure
   */
  @Override
  public void list(NotificationListRequest req, StreamObserver<BulletinReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select id, uuid, category, title,  address_level1, address_level2, publisher, qty from recommend where now() between date1 and date2 ";
      List<String> list = new ArrayList<>();
      boolean flg = false;
      String category = "";
      if (req.getCategory1()) {
        category += " category = ? ";
        list.add("国企");
        flg = true;
      }
      if (req.getCategory2()) {
        if (flg) {
          category += " or ";
        }
        category += "category = ? ";
        list.add("公务员");
        flg = true;
      }
      if (req.getCategory3()) {
        if (flg) {
          category += " or ";
        }
        category += " category = ? ";
        list.add("事业单位");
        flg = true;
      }
      if (req.getCategory4()) {
        if (flg) {
          category += " or ";
        }
        category += " category = ? ";
        list.add("教师");
        flg = true;
      }
      if (req.getCategory5()) {
        if (flg) {
          category += " or ";
        }
        category += " category = ? ";
        list.add("其它");
        flg = true;
      }
      if (flg) {
        sql += "and ( " + category + " ) ";
      }
      try (PreparedStatement ps = conn.prepareStatement(sql + " ORDER BY date2 limit 100")) {
        for (int inx = 0; inx < list.size(); inx++) {
          ps.setString(inx + 1, list.get(inx));
        }
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
  public void get(NotificationGetRequest req, StreamObserver<BulletinReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from recommend where id = ? and uuid = ?";
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
    BulletinReply reply = BulletinReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
