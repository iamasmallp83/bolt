package com.cmex.bolt.server.util;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.ArrayList;

/**
 * HTML性能报告生成器
 * 将性能测试结果生成为美观的HTML报告
 */
public class HTMLReportGenerator {
    
    private final List<TestResult> testResults = new ArrayList<>();
    private String reportTitle = "性能测试报告";
    
    public HTMLReportGenerator() {
    }
    
    public HTMLReportGenerator(String title) {
        this.reportTitle = title;
    }
    
    /**
     * 添加测试结果
     */
    public void addTestResult(String testName, CompletionTracker.CompletionResult result, 
                             long sendElapsedMs, String description) {
        testResults.add(new TestResult(testName, result, sendElapsedMs, description));
    }
    
    /**
     * 生成HTML报告
     */
    public void generateReport(String filename) throws IOException {
        String html = buildHTMLContent();
        
        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(html);
        }
        
        System.out.println("📊 HTML性能报告已生成: " + filename);
    }
    
    /**
     * 构建HTML内容
     */
    private String buildHTMLContent() {

        // HTML头部

        String html = getHTMLHeader() +

                // 报告标题和摘要
                getReportHeader() +

                // 总体统计摘要
                getSummarySection() +

                // 详细测试结果
                getDetailedResults() +

                // 性能图表
                getPerformanceCharts() +

                // 建议和结论
                getRecommendations() +

                // HTML尾部
                getHTMLFooter();
        
        return html;
    }
    
    /**
     * HTML文档头部
     */
    private String getHTMLHeader() {
        String html = "<!DOCTYPE html>\n" +
                "<html lang=\"zh-CN\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>" + reportTitle + "</title>\n" +
                "    <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>\n" +
                "    <style>\n" +
                "        :root {\n" +
                "            --primary-color: #2c3e50;\n" +
                "            --secondary-color: #3498db;\n" +
                "            --success-color: #27ae60;\n" +
                "            --warning-color: #f39c12;\n" +
                "            --danger-color: #e74c3c;\n" +
                "            --light-bg: #ecf0f1;\n" +
                "            --card-shadow: 0 2px 10px rgba(0,0,0,0.1);\n" +
                "        }\n" +
                "        \n" +
                "        * {\n" +
                "            margin: 0;\n" +
                "            padding: 0;\n" +
                "            box-sizing: border-box;\n" +
                "        }\n" +
                "        \n" +
                "        body {\n" +
                "            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;\n" +
                "            line-height: 1.6;\n" +
                "            color: var(--primary-color);\n" +
                "            background-color: var(--light-bg);\n" +
                "        }\n" +
                "        \n" +
                "        .container {\n" +
                "            max-width: 1200px;\n" +
                "            margin: 0 auto;\n" +
                "            padding: 20px;\n" +
                "        }\n" +
                "        \n" +
                "        .header {\n" +
                "            background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));\n" +
                "            color: white;\n" +
                "            padding: 30px 0;\n" +
                "            margin-bottom: 30px;\n" +
                "            border-radius: 10px;\n" +
                "            text-align: center;\n" +
                "        }\n" +
                "        \n" +
                "        .header h1 {\n" +
                "            font-size: 2.5em;\n" +
                "            margin-bottom: 10px;\n" +
                "        }\n" +
                "        \n" +
                "        .header .subtitle {\n" +
                "            font-size: 1.2em;\n" +
                "            opacity: 0.9;\n" +
                "        }\n" +
                "        \n" +
                "        .card {\n" +
                "            background: white;\n" +
                "            border-radius: 10px;\n" +
                "            padding: 25px;\n" +
                "            margin-bottom: 25px;\n" +
                "            box-shadow: var(--card-shadow);\n" +
                "        }\n" +
                "        \n" +
                "        .card h2 {\n" +
                "            color: var(--primary-color);\n" +
                "            margin-bottom: 20px;\n" +
                "            padding-bottom: 10px;\n" +
                "            border-bottom: 2px solid var(--secondary-color);\n" +
                "        }\n" +
                "        \n" +
                "        .metrics-grid {\n" +
                "            display: grid;\n" +
                "            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));\n" +
                "            gap: 20px;\n" +
                "            margin: 20px 0;\n" +
                "        }\n" +
                "        \n" +
                "        .metric-card {\n" +
                "            background: linear-gradient(135deg, #f8f9fa, #e9ecef);\n" +
                "            padding: 20px;\n" +
                "            border-radius: 8px;\n" +
                "            text-align: center;\n" +
                "            border-left: 4px solid var(--secondary-color);\n" +
                "        }\n" +
                "        \n" +
                "        .metric-value {\n" +
                "            font-size: 2em;\n" +
                "            font-weight: bold;\n" +
                "            color: var(--primary-color);\n" +
                "            margin-bottom: 5px;\n" +
                "        }\n" +
                "        \n" +
                "        .metric-label {\n" +
                "            font-size: 0.9em;\n" +
                "            color: #6c757d;\n" +
                "            text-transform: uppercase;\n" +
                "            letter-spacing: 1px;\n" +
                "        }\n" +
                "        \n" +
                "        .status-badge {\n" +
                "            display: inline-block;\n" +
                "            padding: 4px 12px;\n" +
                "            border-radius: 20px;\n" +
                "            font-size: 0.8em;\n" +
                "            font-weight: bold;\n" +
                "            text-transform: uppercase;\n" +
                "        }\n" +
                "        \n" +
                "        .status-excellent { background-color: var(--success-color); color: white; }\n" +
                "        .status-good { background-color: #2ecc71; color: white; }\n" +
                "        .status-acceptable { background-color: var(--warning-color); color: white; }\n" +
                "        .status-poor { background-color: var(--danger-color); color: white; }\n" +
                "        \n" +
                "        .test-result {\n" +
                "            margin-bottom: 30px;\n" +
                "            border: 1px solid #dee2e6;\n" +
                "            border-radius: 8px;\n" +
                "            overflow: hidden;\n" +
                "        }\n" +
                "        \n" +
                "        .test-header {\n" +
                "            background-color: var(--primary-color);\n" +
                "            color: white;\n" +
                "            padding: 15px 20px;\n" +
                "            font-weight: bold;\n" +
                "        }\n" +
                "        \n" +
                "        .test-content {\n" +
                "            padding: 20px;\n" +
                "        }\n" +
                "        \n" +
                "        .progress-bar {\n" +
                "            background-color: #e9ecef;\n" +
                "            border-radius: 10px;\n" +
                "            height: 20px;\n" +
                "            overflow: hidden;\n" +
                "            margin: 10px 0;\n" +
                "        }\n" +
                "        \n" +
                "        .progress-fill {\n" +
                "            height: 100%;\n" +
                "            background: linear-gradient(90deg, var(--success-color), var(--secondary-color));\n" +
                "            transition: width 0.3s ease;\n" +
                "        }\n" +
                "        \n" +
                "        .chart-container {\n" +
                "            position: relative;\n" +
                "            height: 400px;\n" +
                "            margin: 20px 0;\n" +
                "        }\n" +
                "        \n" +
                "        table {\n" +
                "            width: 100%;\n" +
                "            border-collapse: collapse;\n" +
                "            margin: 20px 0;\n" +
                "        }\n" +
                "        \n" +
                "        th, td {\n" +
                "            padding: 12px;\n" +
                "            text-align: left;\n" +
                "            border-bottom: 1px solid #dee2e6;\n" +
                "        }\n" +
                "        \n" +
                "        th {\n" +
                "            background-color: var(--light-bg);\n" +
                "            font-weight: bold;\n" +
                "            color: var(--primary-color);\n" +
                "        }\n" +
                "        \n" +
                "        .recommendation {\n" +
                "            background-color: #fff3cd;\n" +
                "            border: 1px solid #ffeaa7;\n" +
                "            border-radius: 6px;\n" +
                "            padding: 15px;\n" +
                "            margin: 10px 0;\n" +
                "        }\n" +
                "        \n" +
                "        .recommendation .icon {\n" +
                "            display: inline-block;\n" +
                "            width: 20px;\n" +
                "            text-align: center;\n" +
                "            margin-right: 10px;\n" +
                "        }\n" +
                "        \n" +
                "        @media (max-width: 768px) {\n" +
                "            .container {\n" +
                "                padding: 10px;\n" +
                "            }\n" +
                "            \n" +
                "            .metrics-grid {\n" +
                "                grid-template-columns: 1fr;\n" +
                "            }\n" +
                "            \n" +
                "            .header h1 {\n" +
                "                font-size: 2em;\n" +
                "            }\n" +
                "        }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n";
        
        return html;
    }
    
    /**
     * 报告头部
     */
    private String getReportHeader() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        
        return """
<div class="container">
    <div class="header">
        <h1>%s</h1>
        <div class="subtitle">生成时间: %s</div>
    </div>
""".formatted(reportTitle, timestamp);
    }
    
    /**
     * 总体统计摘要
     */
    private String getSummarySection() {
        if (testResults.isEmpty()) {
            return "<div class=\"card\"><h2>📊 总体统计</h2><p>暂无测试数据</p></div>";
        }
        
        // 计算总体统计
        long totalRequests = testResults.stream().mapToLong(r -> r.result.stats().sentRequests()).sum();
        long totalProcessed = testResults.stream().mapToLong(r -> r.result.stats().processedRequests()).sum();
        long totalSuccessful = testResults.stream().mapToLong(r -> r.result.stats().successfulRequests()).sum();
        double avgThroughput = testResults.stream().mapToDouble(r -> r.result.stats().getThroughput()).average().orElse(0);
        double avgLatency = testResults.stream().mapToDouble(r -> r.result.stats().getAverageProcessingTimeMs()).average().orElse(0);
        
        String overallStatus = getOverallPerformanceStatus(avgThroughput, avgLatency);
        
        return """
<div class="card">
    <h2>📊 总体统计摘要</h2>
    <div class="metrics-grid">
        <div class="metric-card">
            <div class="metric-value">%,d</div>
            <div class="metric-label">总请求数</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">%,d</div>
            <div class="metric-label">已处理请求</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">%,d</div>
            <div class="metric-label">成功请求</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">%.0f</div>
            <div class="metric-label">平均吞吐量 (req/s)</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">%.3f</div>
            <div class="metric-label">平均延迟 (ms)</div>
        </div>
        <div class="metric-card">
            <div class="metric-value"><span class="%s">%s</span></div>
            <div class="metric-label">整体性能等级</div>
        </div>
    </div>
    
    <div class="progress-bar">
        <div class="progress-fill" style="width: %.1f%%"></div>
    </div>
    <p style="text-align: center; margin-top: 10px;">
        总体完成率: %.1f%% (%,d / %,d)
    </p>
</div>
""".formatted(
    totalRequests, totalProcessed, totalSuccessful, avgThroughput, avgLatency,
    getStatusClass(overallStatus), overallStatus,
    totalRequests > 0 ? (double) totalProcessed / totalRequests * 100 : 0,
    totalRequests > 0 ? (double) totalProcessed / totalRequests * 100 : 0,
    totalProcessed, totalRequests
);
    }
    
    /**
     * 详细测试结果
     */
    private String getDetailedResults() {
        StringBuilder html = new StringBuilder();
        html.append("<div class=\"card\"><h2>📈 详细测试结果</h2>");
        
        for (int i = 0; i < testResults.size(); i++) {
            TestResult result = testResults.get(i);
            html.append(generateTestResultHTML(result, i + 1));
        }
        
        html.append("</div>");
        return html.toString();
    }
    
    /**
     * 生成单个测试结果HTML
     */
    private String generateTestResultHTML(TestResult testResult, int index) {
        CompletionTracker.CompletionStats stats = testResult.result.stats();
        String performanceLevel = getPerformanceLevel(stats.getThroughput());
        String latencyLevel = getLatencyLevel(stats.getAverageProcessingTimeMs());
        
        return """
<div class="test-result">
    <div class="test-header">
        测试 #%d: %s
    </div>
    <div class="test-content">
        <p><strong>描述:</strong> %s</p>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-value">%,d</div>
                <div class="metric-label">发送请求</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">%,d</div>
                <div class="metric-label">处理请求</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">%,d</div>
                <div class="metric-label">成功请求</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">%.2f ms</div>
                <div class="metric-label">发送耗时</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">%.2f ms</div>
                <div class="metric-label">处理耗时</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">%.6f ms</div>
                <div class="metric-label">平均处理时间</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">%.0f req/s</div>
                <div class="metric-label">吞吐量</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">%.1f%%</div>
                <div class="metric-label">成功率</div>
            </div>
        </div>
        
        <div style="margin-top: 20px;">
            <span class="status-badge %s">%s</span>
            <span class="status-badge %s">%s</span>
        </div>
        
        <div class="progress-bar">
            <div class="progress-fill" style="width: %.1f%%"></div>
        </div>
        <p style="text-align: center; margin-top: 5px;">完成率: %.1f%%</p>
    </div>
</div>
""".formatted(
    index, testResult.testName, testResult.description,
    stats.sentRequests(), stats.processedRequests(), stats.successfulRequests(),
    (double) testResult.sendElapsedMs, stats.getProcessingElapsedMs(), stats.getAverageProcessingTimeMs(),
    stats.getThroughput(), stats.getSuccessRate() * 100,
    getStatusClass(performanceLevel), performanceLevel,
    getStatusClass(latencyLevel), latencyLevel,
    stats.getCompletionRate() * 100, stats.getCompletionRate() * 100
);
    }
    
    /**
     * 性能图表
     */
    private String getPerformanceCharts() {
        if (testResults.isEmpty()) {
            return "";
        }
        
        StringBuilder labels = new StringBuilder();
        StringBuilder throughputData = new StringBuilder();
        StringBuilder latencyData = new StringBuilder();
        
        for (int i = 0; i < testResults.size(); i++) {
            TestResult result = testResults.get(i);
            if (i > 0) {
                labels.append(",");
                throughputData.append(",");
                latencyData.append(",");
            }
            labels.append("'").append(result.testName).append("'");
            throughputData.append(String.format("%.0f", result.result.stats().getThroughput()));
            latencyData.append(String.format("%.6f", result.result.stats().getAverageProcessingTimeMs()));
        }
        
        return """
<div class="card">
    <h2>📊 性能图表</h2>
    
    <div class="chart-container">
        <canvas id="throughputChart"></canvas>
    </div>
    
    <div class="chart-container">
        <canvas id="latencyChart"></canvas>
    </div>
</div>

<script>
// 吞吐量图表
const throughputCtx = document.getElementById('throughputChart').getContext('2d');
new Chart(throughputCtx, {
    type: 'bar',
    data: {
        labels: [%s],
        datasets: [{
            label: '吞吐量 (requests/sec)',
            data: [%s],
            backgroundColor: 'rgba(52, 152, 219, 0.8)',
            borderColor: 'rgba(52, 152, 219, 1)',
            borderWidth: 1
        }]
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            title: {
                display: true,
                text: '各测试吞吐量对比'
            }
        },
        scales: {
            y: {
                beginAtZero: true,
                title: {
                    display: true,
                    text: 'Requests/Second'
                }
            }
        }
    }
});

// 延迟图表
const latencyCtx = document.getElementById('latencyChart').getContext('2d');
new Chart(latencyCtx, {
    type: 'line',
    data: {
        labels: [%s],
        datasets: [{
            label: '平均延迟 (ms)',
            data: [%s],
            backgroundColor: 'rgba(231, 76, 60, 0.2)',
            borderColor: 'rgba(231, 76, 60, 1)',
            borderWidth: 2,
            fill: true
        }]
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            title: {
                display: true,
                text: '各测试平均延迟对比'
            }
        },
        scales: {
            y: {
                beginAtZero: true,
                title: {
                    display: true,
                    text: 'Milliseconds'
                }
            }
        }
    }
});
</script>
""".formatted(labels, throughputData, labels, latencyData);
    }
    
    /**
     * 建议和结论
     */
    private String getRecommendations() {
        StringBuilder recommendations = new StringBuilder();
        recommendations.append("<div class=\"card\"><h2>💡 优化建议</h2>");
        
        // 分析测试结果并生成建议
        for (TestResult result : testResults) {
            CompletionTracker.CompletionStats stats = result.result.stats();
            
            if (stats.getRejectionRate() > 0.1) {
                recommendations.append("""
<div class="recommendation">
    <span class="icon">⚠️</span>
    <strong>%s:</strong> 拒绝率较高 (%.1f%%)，建议增加RingBuffer容量或降低发送速率
</div>
""".formatted(result.testName, stats.getRejectionRate() * 100));
            }
            
            if (stats.getAverageProcessingTimeMs() > 10) {
                recommendations.append("""
<div class="recommendation">
    <span class="icon">🐌</span>
    <strong>%s:</strong> 平均延迟较高 (%.3f ms)，建议优化处理逻辑或增加消费者线程
</div>
""".formatted(result.testName, stats.getAverageProcessingTimeMs()));
            }
            
            if (stats.getThroughput() < 10000) {
                recommendations.append("""
<div class="recommendation">
    <span class="icon">📈</span>
    <strong>%s:</strong> 吞吐量偏低 (%.0f req/s)，建议检查系统瓶颈
</div>
""".formatted(result.testName, stats.getThroughput()));
            }
        }
        
        if (recommendations.toString().endsWith("<h2>💡 优化建议</h2>")) {
            recommendations.append("<p>🎉 所有测试结果良好，系统性能优秀！</p>");
        }
        
        recommendations.append("</div>");
        return recommendations.toString();
    }
    
    /**
     * HTML文档尾部
     */
    private String getHTMLFooter() {
        return """
</div>
</body>
</html>
""";
    }
    
    /**
     * 获取性能等级
     */
    private String getPerformanceLevel(double throughput) {
        if (throughput > 50000) return "EXCELLENT";
        if (throughput > 20000) return "GOOD";
        if (throughput > 10000) return "ACCEPTABLE";
        return "POOR";
    }
    
    /**
     * 获取延迟等级
     */
    private String getLatencyLevel(double latency) {
        if (latency < 1.0) return "EXCELLENT";
        if (latency < 5.0) return "GOOD";
        if (latency < 10.0) return "ACCEPTABLE";
        return "HIGH";
    }
    
    /**
     * 获取整体性能状态
     */
    private String getOverallPerformanceStatus(double avgThroughput, double avgLatency) {
        String throughputLevel = getPerformanceLevel(avgThroughput);
        String latencyLevel = getLatencyLevel(avgLatency);
        
        if ("EXCELLENT".equals(throughputLevel) && "EXCELLENT".equals(latencyLevel)) return "EXCELLENT";
        if (("EXCELLENT".equals(throughputLevel) || "GOOD".equals(throughputLevel)) && 
            ("EXCELLENT".equals(latencyLevel) || "GOOD".equals(latencyLevel))) return "GOOD";
        if (!"POOR".equals(throughputLevel) && !"HIGH".equals(latencyLevel)) return "ACCEPTABLE";
        return "POOR";
    }
    
    /**
     * 获取状态样式类
     */
    private String getStatusClass(String status) {
        return switch (status) {
            case "EXCELLENT" -> "status-excellent";
            case "GOOD" -> "status-good";
            case "ACCEPTABLE" -> "status-acceptable";
            default -> "status-poor";
        };
    }
    
    /**
     * 测试结果记录
     */
    private record TestResult(
        String testName,
        CompletionTracker.CompletionResult result,
        long sendElapsedMs,
        String description
    ) {}
} 