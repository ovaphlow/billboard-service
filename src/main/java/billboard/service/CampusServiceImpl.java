package billboard.service;

import java.sql.Connection;
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

public class CampusServiceImpl extends CampusGrpc.CampusImplBase {
  private static final Logger logger = LoggerFactory.getLogger(CampusServiceImpl.class);

  @Override
  public void get(CampusGetRequest req, StreamObserver<CampusReply> responseObserver) {
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from campus where id = ? and uuid = ? ";
      Map<String, Object> result = new QueryRunner().query(conn, sql,
          new MapHandler(),
          req.getId(),
          req.getUuid());
      resp.put("content", result);
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CampusReply reply = CampusReply
        .newBuilder()
        .setData(new Gson().toJson(resp))
        .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void search(CampusSearchRequest req, StreamObserver<CampusReply> responseObserver) {
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection cnx = Persistence.getConn()) {
      String sql = """
          select id, uuid, title, address_level3, address_level2, date, school, category
          from campus
          where
            date >= curdate()
            and position(? in address_level2) > 0
            and (category = ? or category = ?)
            and (position(? in title) > 0 or position(? in school) > 0)
          order by date
          limit ?, 20
          """;
      int page = Integer.parseInt(req.getFilterMap().get("page"));
      int offset = page > 1 ? (page - 1) * 20 : 0;
      List<Map<String, Object>> result = new QueryRunner().query(cnx, sql,
          new MapListHandler(),
          req.getFilterMap().get("city"),
          Boolean.parseBoolean(req.getFilterMap().get("category1")) ? "宣讲会" : "",
          Boolean.parseBoolean(req.getFilterMap().get("category2")) ? "双选会" : "",
          req.getFilterMap().get("keyword"),
          req.getFilterMap().get("keyword"),
          offset);
      resp.put("content", result);
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    CampusReply reply = CampusReply.newBuilder().setData(new Gson().toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
