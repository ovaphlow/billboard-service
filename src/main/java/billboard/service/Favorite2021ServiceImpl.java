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

public class Favorite2021ServiceImpl extends Favorite2021Grpc.Favorite2021ImplBase {
  private static final Logger logger = LoggerFactory.getLogger(Favorite2021ServiceImpl.class);

  @Override
  public void filter(Favorite2021FilterRequest req, StreamObserver<MiscReply> responseObserver) {
    String resp = "[]";
    try (Connection cnx = Persistence.getConn()) {
      if ("by-candidate-id-list".equals(req.getOption())) {
        String sql = """
            select id, category1, category2, data_id, data_uuid
            from favorite
            where user_id = ?
              and category1 = '个人用户'
            """;
        List<Map<String, Object>> result = new QueryRunner().query(cnx, sql, new MapListHandler(),
            req.getDataMap().get("keyword"),
            req.getDataMap().get("keyword"));
        resp = new Gson().toJson(result);
      }
    } catch (Exception e) {
      logger.error("", e);
    }
    MiscReply reply = MiscReply.newBuilder().setData(resp).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();

  }
}
