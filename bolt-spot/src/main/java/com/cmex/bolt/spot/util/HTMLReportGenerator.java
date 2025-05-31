package com.cmex.bolt.spot.util;

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
        StringBuilder html = new StringBuilder();
        
        // HTML头部
        html.append(getHTMLHeader());
        
        // 报告标题和摘要
        html.append(getReportHeader());
        
        // 总体统计摘要
        html.append(getSummarySection());
        
        // 详细测试结果
        html.append(getDetailedResults());
        
        // 性能图表
        html.append(getPerformanceCharts());
        
        // 建议和结论
        html.append(getRecommendations());
        
        // HTML尾部
        html.append(getHTMLFooter());
        
        return html.toString();
    }
    
    /**
     * HTML文档头部
     */
    private String getHTMLHeader() {
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n");
        html.append("<html lang=\"zh-CN\">\n");
        html.append("<head>\n");
        html.append("    <meta charset=\"UTF-8\">\n");
        html.append("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
        html.append("    <title>").append(reportTitle).append("</title>\n");
        html.append("    <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>\n");
        html.append("    <style>\n");
        html.append("        :root {\n");
        html.append("            --primary-color: #2c3e50;\n");
        html.append("            --secondary-color: #3498db;\n");
        html.append("            --success-color: #27ae60;\n");
        html.append("            --warning-color: #f39c12;\n");
        html.append("            --danger-color: #e74c3c;\n");
        html.append("            --light-bg: #ecf0f1;\n");
        html.append("            --card-shadow: 0 2px 10px rgba(0,0,0,0.1);\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        * {\n");
        html.append("            margin: 0;\n");
        html.append("            padding: 0;\n");
        html.append("            box-sizing: border-box;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        body {\n");
        html.append("            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;\n");
        html.append("            line-height: 1.6;\n");
        html.append("            color: var(--primary-color);\n");
        html.append("            background-color: var(--light-bg);\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .container {\n");
        html.append("            max-width: 1200px;\n");
        html.append("            margin: 0 auto;\n");
        html.append("            padding: 20px;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .header {\n");
        html.append("            background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));\n");
        html.append("            color: white;\n");
        html.append("            padding: 30px 0;\n");
        html.append("            margin-bottom: 30px;\n");
        html.append("            border-radius: 10px;\n");
        html.append("            text-align: center;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .header h1 {\n");
        html.append("            font-size: 2.5em;\n");
        html.append("            margin-bottom: 10px;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .header .subtitle {\n");
        html.append("            font-size: 1.2em;\n");
        html.append("            opacity: 0.9;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .card {\n");
        html.append("            background: white;\n");
        html.append("            border-radius: 10px;\n");
        html.append("            padding: 25px;\n");
        html.append("            margin-bottom: 25px;\n");
        html.append("            box-shadow: var(--card-shadow);\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .card h2 {\n");
        html.append("            color: var(--primary-color);\n");
        html.append("            margin-bottom: 20px;\n");
        html.append("            padding-bottom: 10px;\n");
        html.append("            border-bottom: 2px solid var(--secondary-color);\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .metrics-grid {\n");
        html.append("            display: grid;\n");
        html.append("            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));\n");
        html.append("            gap: 20px;\n");
        html.append("            margin: 20px 0;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .metric-card {\n");
        html.append("            background: linear-gradient(135deg, #f8f9fa, #e9ecef);\n");
        html.append("            padding: 20px;\n");
        html.append("            border-radius: 8px;\n");
        html.append("            text-align: center;\n");
        html.append("            border-left: 4px solid var(--secondary-color);\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .metric-value {\n");
        html.append("            font-size: 2em;\n");
        html.append("            font-weight: bold;\n");
        html.append("            color: var(--primary-color);\n");
        html.append("            margin-bottom: 5px;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .metric-label {\n");
        html.append("            font-size: 0.9em;\n");
        html.append("            color: #6c757d;\n");
        html.append("            text-transform: uppercase;\n");
        html.append("            letter-spacing: 1px;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .status-badge {\n");
        html.append("            display: inline-block;\n");
        html.append("            padding: 4px 12px;\n");
        html.append("            border-radius: 20px;\n");
        html.append("            font-size: 0.8em;\n");
        html.append("            font-weight: bold;\n");
        html.append("            text-transform: uppercase;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .status-excellent { background-color: var(--success-color); color: white; }\n");
        html.append("        .status-good { background-color: #2ecc71; color: white; }\n");
        html.append("        .status-acceptable { background-color: var(--warning-color); color: white; }\n");
        html.append("        .status-poor { background-color: var(--danger-color); color: white; }\n");
        html.append("        \n");
        html.append("        .test-result {\n");
        html.append("            margin-bottom: 30px;\n");
        html.append("            border: 1px solid #dee2e6;\n");
        html.append("            border-radius: 8px;\n");
        html.append("            overflow: hidden;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .test-header {\n");
        html.append("            background-color: var(--primary-color);\n");
        html.append("            color: white;\n");
        html.append("            padding: 15px 20px;\n");
        html.append("            font-weight: bold;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .test-content {\n");
        html.append("            padding: 20px;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .progress-bar {\n");
        html.append("            background-color: #e9ecef;\n");
        html.append("            border-radius: 10px;\n");
        html.append("            height: 20px;\n");
        html.append("            overflow: hidden;\n");
        html.append("            margin: 10px 0;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .progress-fill {\n");
        html.append("            height: 100%;\n");
        html.append("            background: linear-gradient(90deg, var(--success-color), var(--secondary-color));\n");
        html.append("            transition: width 0.3s ease;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .chart-container {\n");
        html.append("            position: relative;\n");
        html.append("            height: 400px;\n");
        html.append("            margin: 20px 0;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        table {\n");
        html.append("            width: 100%;\n");
        html.append("            border-collapse: collapse;\n");
        html.append("            margin: 20px 0;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        th, td {\n");
        html.append("            padding: 12px;\n");
        html.append("            text-align: left;\n");
        html.append("            border-bottom: 1px solid #dee2e6;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        th {\n");
        html.append("            background-color: var(--light-bg);\n");
        html.append("            font-weight: bold;\n");
        html.append("            color: var(--primary-color);\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .recommendation {\n");
        html.append("            background-color: #fff3cd;\n");
        html.append("            border: 1px solid #ffeaa7;\n");
        html.append("            border-radius: 6px;\n");
        html.append("            padding: 15px;\n");
        html.append("            margin: 10px 0;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        .recommendation .icon {\n");
        html.append("            display: inline-block;\n");
        html.append("            width: 20px;\n");
        html.append("            text-align: center;\n");
        html.append("            margin-right: 10px;\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        @media (max-width: 768px) {\n");
        html.append("            .container {\n");
        html.append("                padding: 10px;\n");
        html.append("            }\n");
        html.append("            \n");
        html.append("            .metrics-grid {\n");
        html.append("                grid-template-columns: 1fr;\n");
        html.append("            }\n");
        html.append("            \n");
        html.append("            .header h1 {\n");
        html.append("                font-size: 2em;\n");
        html.append("            }\n");
        html.append("        }\n");
        html.append("    </style>\n");
        html.append("</head>\n");
        html.append("<body>\n");
        
        return html.toString();
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