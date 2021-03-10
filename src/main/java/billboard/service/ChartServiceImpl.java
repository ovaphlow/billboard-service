package billboard.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// @SuppressWarnings("unchecked")
public class ChartServiceImpl extends ChartGrpc.ChartImplBase {
  private static final Logger logger = LoggerFactory.getLogger(ChartServiceImpl.class);

  @Override
  public void entHome(ChartEntHomeRequest req, StreamObserver<MiscReply> responseObserver) {
    Gson gson = new Gson();
    Map<String, Object> resp = new HashMap<>();
    resp.put("message", "");
    resp.put("content", "");
    try (Connection conn = Persistence.getConn()) {
      String sql = """
          select zhiwei, count(zhiwei) as count
          from (select qiwangzhiwei as zhiwei from resume where qiwanghangye != '' and  qiwangzhiwei != '' ) as t
          GROUP BY zhiwei
          ORDER BY count DESC
          limit 10
          """;
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ResultSet rs = ps.executeQuery();
        List<Map<String, Object>> result = Persistence.getList(rs);
        resp.put("content", result);
      }
    } catch (Exception e) {
      logger.error("", e);
      resp.put("message", "gRPC服务器错误");
    }
    MiscReply reply = MiscReply.newBuilder().setData(gson.toJson(resp)).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

}
