# 使用 OpenJDK 17 作为基础镜像
FROM openjdk:17-jdk-slim

# 设置工作目录
WORKDIR /app

# 复制构建的 JAR 文件
COPY bolt-server/build/libs/bolt-server-1.0-SNAPSHOT.jar app.jar

# 暴露端口
EXPOSE 9090 8080

# 设置 JVM 参数
ENV JAVA_OPTS="-Xmx2g -Xms1g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# 启动命令
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar $0 $@"]
