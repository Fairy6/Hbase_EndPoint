package com.hbase.demo.endpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.hbase.demo.endpoint.Sum.SumRequest;
import com.hbase.demo.endpoint.Sum.SumResponse;
import com.hbase.demo.endpoint.Sum.SumService;
 
/**
 * @author wr
 * 说明：hbase协处理器endpooint的服务端代码
 * 功能：继承通过protocol buffer生成的rpc接口，在服务端获取指定列的数据后进行求和操作，最后将结果返回客户端
 */
public class SumEndPoint extends SumService implements Coprocessor,CoprocessorService {
    
    private RegionCoprocessorEnvironment env;   // 定义环境
    
    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void getSum(RpcController controller, SumRequest request, RpcCallback<SumResponse> done) {
        // 定义变量
        SumResponse response = null;
        InternalScanner scanner = null;
        // 设置扫描对象
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(request.getFamily()));
        scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes(request.getColumn()));
        // 扫描每个region，取值后求和
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;
            Long sum = 0L;
            do {
                hasMore = scanner.next(results);
                for (Cell cell : results) {
                    sum += Long.parseLong(new String(CellUtil.cloneValue(cell)));
                }
                results.clear();
            } while (hasMore);
            // 设置返回结果
            response = SumResponse.newBuilder().setSum(sum).build();
        } catch (IOException e) {
            ResponseConverter.setControllerException(controller, e);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // 将rpc结果返回给客户端
        done.run(response);
    }
    
    // 协处理器初始化时调用的方法
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("no load region");
        }
    }
    
    // 协处理器结束时调用的方法
    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        
    }

}
