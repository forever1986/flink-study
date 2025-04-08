package com.demo.lesson06.custom;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * 真正运行在taskManager的代码
 */
public class MinIoSinkWriter implements SinkWriter<String> {

    // minIO的客户端
    private final MinioClient minioClient;
    // 桶
    private final String bucket;
    // 子任务Id
    private final int subtaskId;

    public MinIoSinkWriter(String endpoint, String accessKey, String secretKey, String bucket, int subtaskId) {
        this.minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
        this.bucket = bucket;
        this.subtaskId = subtaskId;
    }

    /**
     * 每次有数据过来时，写入到minIO
     */
    @Override
    public void write(String element, Context context) throws IOException, InterruptedException {
        try {
            String objectName = String.format("flink-%d-%d.txt", subtaskId, System.currentTimeMillis());
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(objectName)
                    .stream(new ByteArrayInputStream(element.getBytes()), element.length(), -1)
                    .build());
        } catch (Exception e) {
            throw new RuntimeException("Upload to MinIO failed", e);
        }
    }

    /**
     * 在检查点开启或者输入结束时调用
     */
    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        System.out.println("flush"+endOfInput);
    }

    /**
     * 子任务销毁时调用
     */
    @Override
    public void close() throws Exception {
        // 可以用于关闭不必要资源，这里minioClient无需关闭，则不用写什么内容
    }
}

