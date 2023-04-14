package org.example.redis;

import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author : cxm
 * @date : 2023/4/7 上午10:12
 * @Description : redis工具类，基于lettuce操作redis
 */
public class RedisUtils implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisUtils.class);

    public Properties getRedisConfig(Configuration configuration) {
        Properties props = new Properties();
        // Redis连接参数
        ConfigOption<String> nodes = ConfigOptions.key("redis.nodes").stringType().noDefaultValue();
        props.setProperty("redis.nodes", configuration.getString(nodes));
        ConfigOption<String> auth = ConfigOptions.key("redis.auth").stringType().noDefaultValue();
        props.setProperty("redis.auth", configuration.getString(auth));
        ConfigOption<String> client = ConfigOptions.key("redis.client").stringType().noDefaultValue();
        props.setProperty("redis.client", configuration.getString(client));
        ConfigOption<String> maxRedirects = ConfigOptions.key("redis.maxRedirects").stringType().noDefaultValue();
        props.setProperty("redis.maxRedirects", configuration.getString(maxRedirects));
        ConfigOption<String> testWhileIdle = ConfigOptions.key("redis.testWhileIdle").stringType().noDefaultValue();
        props.setProperty("redis.testWhileIdle", configuration.getString(testWhileIdle));
        ConfigOption<String> minEvictableIdleTimeMillis = ConfigOptions.key("redis.minEvictableIdleTimeMillis").stringType().noDefaultValue();
        props.setProperty("redis.minEvictableIdleTimeMillis", configuration.getString(minEvictableIdleTimeMillis));
        ConfigOption<String> timeBetweenEvictionRunsMillis = ConfigOptions.key("redis.timeBetweenEvictionRunsMillis").stringType().noDefaultValue();
        props.setProperty("redis.timeBetweenEvictionRunsMillis", configuration.getString(timeBetweenEvictionRunsMillis));
        ConfigOption<String> numTestsPerEvictionRun = ConfigOptions.key("redis.numTestsPerEvictionRun").stringType().noDefaultValue();
        props.setProperty("redis.numTestsPerEvictionRun", configuration.getString(numTestsPerEvictionRun));
        ConfigOption<String> testOnBorrow = ConfigOptions.key("redis.testOnBorrow").stringType().noDefaultValue();
        props.setProperty("redis.testOnBorrow", configuration.getString(testOnBorrow));
        ConfigOption<String> testOnReturn = ConfigOptions.key("redis.testOnReturn").stringType().noDefaultValue();
        props.setProperty("redis.testOnReturn", configuration.getString(testOnReturn));
        ConfigOption<String> testOnCreate = ConfigOptions.key("redis.testOnCreate").stringType().noDefaultValue();
        props.setProperty("redis.testOnCreate", configuration.getString(testOnCreate));
        ConfigOption<String> maxTotal = ConfigOptions.key("redis.maxTotal").stringType().noDefaultValue();
        props.setProperty("redis.maxTotal", configuration.getString(maxTotal));
        ConfigOption<String> maxIdle = ConfigOptions.key("redis.maxIdle").stringType().noDefaultValue();
        props.setProperty("redis.maxIdle", configuration.getString(maxIdle));
        ConfigOption<String> minIdle = ConfigOptions.key("redis.minIdle").stringType().noDefaultValue();
        props.setProperty("redis.minIdle", configuration.getString(minIdle));
        ConfigOption<String> maxWaitMillis = ConfigOptions.key("redis.maxWaitMillis").stringType().noDefaultValue();
        props.setProperty("redis.maxWaitMillis", configuration.getString(maxWaitMillis));

        return props;
    }

    public RedisTemplate<String, String> buildRedisTemplate(Properties properties) {
        List<RedisNode> nodeList = new ArrayList<>();
        String[] nodes = properties.getProperty("redis.nodes").split(",");
        for (String node : nodes) {
            String[] hostAndPort = node.split(":");
            if (hostAndPort.length < 1) {
                LOGGER.error("Redis hosts not in correct format of <host>:<port> [$hostPorts]");
            } else {
                nodeList.add(new RedisNode(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            }
        }
        // rediscluster配置
        RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
        redisClusterConfiguration.setClusterNodes(nodeList);
        redisClusterConfiguration.setPassword(properties.getProperty("redis.auth"));
        redisClusterConfiguration.setMaxRedirects(Integer.parseInt(properties.getProperty("redis.maxRedirects")));
        // redispool连接池配置
        GenericObjectPoolConfig<String> genericObjectPoolConfig = new GenericObjectPoolConfig<>();
        genericObjectPoolConfig.setTestWhileIdle(Boolean.parseBoolean(properties.getProperty("redis.testWhileIdle")));
        genericObjectPoolConfig.setMinEvictableIdleTimeMillis(Integer.parseInt(properties.getProperty("redis.minEvictableIdleTimeMillis")));
        genericObjectPoolConfig.setNumTestsPerEvictionRun(Integer.parseInt(properties.getProperty("redis.numTestsPerEvictionRun")));
        genericObjectPoolConfig.setTestOnBorrow(Boolean.parseBoolean(properties.getProperty("redis.testOnBorrow")));
        genericObjectPoolConfig.setTestOnReturn(Boolean.parseBoolean(properties.getProperty("redis.testOnReturn")));
        genericObjectPoolConfig.setTestOnCreate(Boolean.parseBoolean(properties.getProperty("redis.testOnCreate")));
        genericObjectPoolConfig.setMaxTotal(Integer.parseInt(properties.getProperty("redis.maxTotal")));
        genericObjectPoolConfig.setMaxIdle(Integer.parseInt(properties.getProperty("redis.maxIdle")));
        genericObjectPoolConfig.setMinIdle(Integer.parseInt(properties.getProperty("redis.minIdle")));
        genericObjectPoolConfig.setMaxWaitMillis(Integer.parseInt(properties.getProperty("redis.maxWaitMillis")));
        // 拓扑刷新
        ClusterTopologyRefreshOptions clusterTopologyRefreshOptions = ClusterTopologyRefreshOptions
                .builder()
                .enableAdaptiveRefreshTrigger()
                .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(30L))
                .enablePeriodicRefresh()
                .build();
        ClusterClientOptions clusterClientOptions = ClusterClientOptions
                .builder()
                .topologyRefreshOptions(clusterTopologyRefreshOptions)
                .build();
        LettucePoolingClientConfiguration lettucePoolingClientConfiguration = LettucePoolingClientConfiguration
                .builder()
                .poolConfig(genericObjectPoolConfig)
                .clientOptions(clusterClientOptions)
                .commandTimeout(Duration.ofSeconds(3L))
                .clientName(properties.getProperty("redis.client"))
                .build();
        LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(redisClusterConfiguration, lettucePoolingClientConfiguration);
        lettuceConnectionFactory.afterPropertiesSet();

        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.setConnectionFactory(lettuceConnectionFactory);
        template.afterPropertiesSet();

        return template;
    }
}
