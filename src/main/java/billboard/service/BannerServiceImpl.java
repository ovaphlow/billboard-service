package billboard.service;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import com.google.gson.Gson;

import io.grpc.stub.StreamObserver;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BannerServiceImpl extends BannerGrpc.BannerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(BannerServiceImpl.class);

  @Override
  public void get(BannerProto.GetRequest req, StreamObserver<BannerProto.Reply> responseObserver) {
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = """
        select id, uuid, datime, data_url, source_url, category
        from banner
        where category = ?
          and status = '启用'
        ORDER BY datime DESC
        """;
      List<Map<String, Object>> result = new QueryRunner().query(conn, sql,
          new MapListHandler(),
          req.getCategory());
      resp.put("content", result);
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BannerProto.Reply reply = BannerProto.Reply
        .newBuilder()
        .setData(new Gson().toJson(resp))
        .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void detail(BannerProto.DetailRequest req,
      StreamObserver<BannerProto.Reply> responseObserver) {
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = "select * from banner where id = ? and uuid = ?";
      Map<String, Object> result = new QueryRunner().query(conn, sql,
          new MapHandler(),
          req.getId(),
          req.getUuid());
      resp.put("content", result);
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    BannerProto.Reply reply = BannerProto.Reply
        .newBuilder()
        .setData(new Gson().toJson(resp))
        .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
