package com.demo.lesson06.custom2;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 上游来一个Tuple2，包括文件名和数据
 * sink写入到
 */
public class MinIoSinkWriter implements StatefulSinkWriter<Tuple2<String, String>, MinIOWriterState> , CheckpointListener {

    // minIO的客户端
    private final MinioClient minioClient;
    // 桶
    private final String bucket;
    // 子任务Id
    private final int subtaskId;
//    // 当前等待写入的数据
//    private final Queue<Tuple2<String,byte[]>> waitQueue = new ConcurrentLinkedDeque<>();

    /**
     * 用于存储已读取完成的数据,可用于删除
     */
    private final List<MinIOWriterState> finishData = new ArrayList<>();

    /**
     * 用于存储已读取完成的数据,可用于删除
     */
    private final List<MinIOWriterState> snapshotData = new ArrayList<>();

    public MinIoSinkWriter(String endpoint, String accessKey, String secretKey, String bucket, int subtaskId) {
        this.minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
        this.bucket = bucket;
        this.subtaskId = subtaskId;
    }

    /**
     * 每次有数据过来时，将数据放入等待队列
     */
    @Override
    public void write(Tuple2<String, String> element, Context context) throws IOException, InterruptedException {
        System.out.println("MinIoSinkWriter.write: "+element);
        try {
            String objectName = String.format("flink-%s.txt", element.f0);
//            if("MinIO".equals(element.f0)){
//                throw new RuntimeException("error");
//            }
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(objectName)
                    .stream(new ByteArrayInputStream(element.f1.getBytes()), element.f1.length(), -1)
                    .build());
            synchronized (finishData) {
                finishData.add(new MinIOWriterState(subtaskId, element));
            }
        } catch (Exception e) {
            // 写入minIO失败时，数据加回去
            throw new RuntimeException("Upload to MinIO failed", e);
        }
    }

    /**
     * 在检查点或者输入结束时调用
     */
    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        System.out.println("MinIoSinkWriter.flush："+endOfInput);
    }

    /**
     * 子任务销毁时调用
     */
    @Override
    public void close() throws Exception {
        // 可以用于关闭不必要资源，这里minioClient无需关闭，则不用写什么内容
    }

    /**
     * 检查点
     */
    @Override
    public List<MinIOWriterState> snapshotState(long checkpointId) throws IOException {
        System.out.println("MinIoSinkWriter.snapshotState");
        synchronized (finishData) {
            snapshotData.addAll(finishData);
            finishData.clear();
        }
        return snapshotData;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        System.out.println("MinIoSinkWriter.notifyCheckpointComplete");
        snapshotData.clear();
    }
}

