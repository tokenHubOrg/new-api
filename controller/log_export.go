package controller

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
)

const maxExportRows = 10000
const exportQueryTimeout = 30 * time.Second
const exportRateLimitDuration = 60 // seconds
const maxConcurrentExports = 5

// exportSemaphore limits concurrent export operations
var exportSemaphore = make(chan struct{}, maxConcurrentExports)

func ExportAllLogs(c *gin.Context) {
	exportLogs(c, true)
}

func ExportUserLogs(c *gin.Context) {
	exportLogs(c, false)
}

func exportLogs(c *gin.Context, isAdmin bool) {
	userID := c.GetInt("id")
	lang := i18n.GetLangFromContext(c)

	// Rate limiting: per-user, 1 export per 60 seconds
	if !checkExportRateLimit(c, userID, lang) {
		return
	}

	// Try to acquire semaphore (non-blocking)
	select {
	case exportSemaphore <- struct{}{}:
		defer func() { <-exportSemaphore }()
	default:
		c.JSON(http.StatusTooManyRequests, gin.H{
			"success": false,
			"message": i18n.Translate(lang, "log_export.concurrent_limit"),
		})
		return
	}

	// Create context with timeout for database query
	ctx, cancel := context.WithTimeout(c.Request.Context(), exportQueryTimeout)
	defer cancel()

	// Parse query parameters
	page, _ := strconv.Atoi(c.Query("p"))
	if page < 0 {
		page = 0
	}

	// Fetch logs with timeout
	logs, total, err := fetchLogsWithTimeout(ctx, c, isAdmin, userID, maxExportRows, page*maxExportRows)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			c.JSON(http.StatusRequestTimeout, gin.H{
				"success": false,
				"message": i18n.Translate(lang, "log_export.query_timeout"),
			})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"message": i18n.Translate(lang, "common.database_error"),
			})
		}
		return
	}

	// Set response headers
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("logs_%s.csv.gz", timestamp)
	c.Header("Content-Type", "application/gzip")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	c.Header("X-Export-Count", strconv.Itoa(len(logs)))
	c.Header("X-Export-Total", strconv.FormatInt(total, 10))
	c.Header("X-Export-Truncated", strconv.FormatBool(total > int64(maxExportRows)))

	// Create gzip writer
	gzipWriter := gzip.NewWriter(c.Writer)
	defer gzipWriter.Close()

	// Create CSV writer
	w := csv.NewWriter(gzipWriter)
	defer w.Flush()

	// Write UTF-8 BOM for Excel compatibility
	gzipWriter.Write([]byte{0xEF, 0xBB, 0xBF})

	// Write CSV header with i18n
	writeCSVHeader(w, isAdmin, lang)

	// Write data rows
	for _, log := range logs {
		writeCSVRow(w, log, isAdmin, lang)
	}

	c.Status(http.StatusOK)
}

func checkExportRateLimit(c *gin.Context, userID int, lang string) bool {
	key := fmt.Sprintf("export_log:user:%d", userID)

	if common.RedisEnabled {
		// Use Redis for rate limiting
		val, err := common.RedisGet(key)
		if err == nil && val != "" {
			lastExportTime, _ := strconv.ParseInt(val, 10, 64)
			elapsed := time.Now().Unix() - lastExportTime
			if elapsed < exportRateLimitDuration {
				remaining := exportRateLimitDuration - elapsed
				c.JSON(http.StatusTooManyRequests, gin.H{
					"success": false,
					"message": i18n.Translate(lang, "log_export.rate_limit", map[string]any{
						"Seconds": remaining,
					}),
				})
				return false
			}
		}
		// Set new export timestamp
		common.RedisSet(key, strconv.FormatInt(time.Now().Unix(), 10), exportRateLimitDuration*time.Second)
	} else {
		// Fallback to in-memory rate limiting (less precise)
		// In production with multiple instances, Redis is recommended
	}

	return true
}

func fetchLogsWithTimeout(ctx context.Context, c *gin.Context, isAdmin bool, userID int, limit int, offset int) ([]*model.Log, int64, error) {
	// Build query parameters
	logType, _ := strconv.Atoi(c.Query("type"))
	tokenName := c.Query("token_name")
	modelName := c.Query("model_name")
	startTimestamp, _ := strconv.ParseInt(c.Query("start_timestamp"), 10, 64)
	endTimestamp, _ := strconv.ParseInt(c.Query("end_timestamp"), 10, 64)
	channel, _ := strconv.Atoi(c.Query("channel"))
	username := c.Query("username")
	content := c.Query("content")

	// Call model layer with context
	if isAdmin {
		return model.GetAllLogsWithContext(ctx, startTimestamp, endTimestamp, modelName, content, username, tokenName, logType, channel, limit, offset)
	} else {
		return model.GetUserLogsWithContext(ctx, userID, startTimestamp, endTimestamp, modelName, content, tokenName, logType, limit, offset)
	}
}

func writeCSVHeader(w *csv.Writer, isAdmin bool, lang string) {
	if isAdmin {
		w.Write([]string{
			i18n.Translate(lang, "log_export.id"),
			i18n.Translate(lang, "log_export.time"),
			i18n.Translate(lang, "log_export.username"),
			i18n.Translate(lang, "log_export.token_name"),
			i18n.Translate(lang, "log_export.model_name"),
			i18n.Translate(lang, "log_export.type"),
			i18n.Translate(lang, "log_export.prompt_tokens"),
			i18n.Translate(lang, "log_export.completion_tokens"),
			i18n.Translate(lang, "log_export.quota"),
			i18n.Translate(lang, "log_export.multiplier"),
			i18n.Translate(lang, "log_export.channel_id"),
			i18n.Translate(lang, "log_export.ip"),
			i18n.Translate(lang, "log_export.content"),
		})
	} else {
		w.Write([]string{
			i18n.Translate(lang, "log_export.id"),
			i18n.Translate(lang, "log_export.time"),
			i18n.Translate(lang, "log_export.token_name"),
			i18n.Translate(lang, "log_export.model_name"),
			i18n.Translate(lang, "log_export.type"),
			i18n.Translate(lang, "log_export.prompt_tokens"),
			i18n.Translate(lang, "log_export.completion_tokens"),
			i18n.Translate(lang, "log_export.quota"),
		})
	}
}

func writeCSVRow(w *csv.Writer, log *model.Log, isAdmin bool, lang string) {
	createdTime := time.Unix(log.CreatedAt, 0).Format("2006-01-02 15:04:05")
	logTypeName := getLogTypeName(log.Type, lang)

	// Calculate multiplier: quota / (prompt_tokens + completion_tokens)
	var multiplier float64
	totalTokens := log.PromptTokens + log.CompletionTokens
	if totalTokens > 0 && log.Quota > 0 {
		multiplier = float64(log.Quota) / float64(totalTokens)
	}

	if isAdmin {
		w.Write([]string{
			strconv.Itoa(log.Id),
			createdTime,
			log.Username,
			log.TokenName,
			log.ModelName,
			logTypeName,
			strconv.Itoa(log.PromptTokens),
			strconv.Itoa(log.CompletionTokens),
			strconv.Itoa(log.Quota),
			fmt.Sprintf("%.2f", multiplier),
			strconv.Itoa(log.ChannelId),
			log.Ip,
			log.Content,
		})
	} else {
		w.Write([]string{
			strconv.Itoa(log.Id),
			createdTime,
			log.TokenName,
			log.ModelName,
			logTypeName,
			strconv.Itoa(log.PromptTokens),
			strconv.Itoa(log.CompletionTokens),
			strconv.Itoa(log.Quota),
		})
	}
}

func getLogTypeName(logType int, lang string) string {
	switch logType {
	case model.LogTypeTopup:
		return i18n.Translate(lang, "log_export.type_topup")
	case model.LogTypeConsume:
		return i18n.Translate(lang, "log_export.type_consume")
	case model.LogTypeManage:
		return i18n.Translate(lang, "log_export.type_manage")
	case model.LogTypeSystem:
		return i18n.Translate(lang, "log_export.type_system")
	default:
		return i18n.Translate(lang, "log_export.type_unknown")
	}
}
