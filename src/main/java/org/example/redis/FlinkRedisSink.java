package org.example.redis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.springframework.data.redis.core.RedisConnectionUtils;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author : cxm
 * @date : 2023/4/12 下午7:14
 * @Description : flink数据流下沉到redis
 */
public class FlinkRedisSink extends RichSinkFunction<String> {
    private transient RedisTemplate<String, String> template = null;
    private transient long expire = 3600L;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Configuration configuration = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        ConfigOption<Long> expireOp = ConfigOptions.key("redis.expire").longType().defaultValue(3600L);
        expire = configuration.getLong(expireOp);

        RedisUtils redisUtils = new RedisUtils();
        Properties properties = redisUtils.getRedisConfig(configuration);
        template = redisUtils.buildRedisTemplate(properties);
    }

    @Override
    public void invoke(String value, Context context) {
        String[] kv = value.split("->");
        if (template != null && kv.length > 1) {
            template.opsForValue().set(kv[0], kv[1], expire, TimeUnit.SECONDS);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (template != null && template.getConnectionFactory() != null) {
            RedisConnectionUtils.unbindConnection(template.getConnectionFactory());
        }
    }
}
