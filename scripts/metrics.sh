#!/bin/bash

# Get metrics URL from first argument or use default
METRICS_URL="${1:-localhost:8080/metrics}"

while true; do
    clear
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "               s3dedup Metrics Dashboard"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo

    METRICS=$(curl -s "http://$METRICS_URL")

    get_metric() {
        echo "$METRICS" | grep "^$1 " | awk '{print $NF}'
    }

    get_metric_with_label() {
        echo "$METRICS" | grep "^$1{" | awk '{print $NF}' | head -1
    }

    FILES=$(get_metric "s3dedup_total_files")
    BLOBS=$(get_metric "s3dedup_total_blobs")
    RATIO=$(get_metric "s3dedup_deduplication_ratio")
    SAVINGS=$(get_metric "s3dedup_storage_savings_ratio")
    LOGICAL=$(get_metric "s3dedup_total_logical_size_bytes")
    STORAGE=$(get_metric "s3dedup_total_storage_bytes")
    SAVED=$(get_metric "s3dedup_deduplicated_bytes_saved")
    UPTIME=$(get_metric "s3dedup_uptime_seconds")
    DEDUP_HITS=$(get_metric_with_label "s3dedup_dedup_hits_total")
    DEDUP_MISSES=$(get_metric_with_label "s3dedup_dedup_misses_total")

    # Format bytes
    format_bytes() {
        local bytes=$1
        if [ -z "$bytes" ]; then echo "0 B"; return; fi
        if [ "$bytes" -gt 1073741824 ]; then
            echo "scale=2; $bytes/1073741824" | bc | awk '{printf "%.2f GB", $1}'
        elif [ "$bytes" -gt 1048576 ]; then
            echo "scale=2; $bytes/1048576" | bc | awk '{printf "%.2f MB", $1}'
        elif [ "$bytes" -gt 1024 ]; then
            echo "scale=2; $bytes/1024" | bc | awk '{printf "%.2f KB", $1}'
        else
            echo "$bytes B"
        fi
    }

    # Format uptime
    format_uptime() {
        local secs=$1
        local hours=$((secs / 3600))
        local mins=$(((secs % 3600) / 60))
        echo "${hours}h ${mins}m"
    }

    echo " Storage Statistics"
    echo "────────────────────────────────────────────────────────"
    printf "  Total Files:          %s\n" "${FILES:-0}"
    printf "  Unique Blobs:         %s\n" "${BLOBS:-0}"
    printf "  Dedup Ratio:          %.2f%%\n" "$(echo "scale=2; ${RATIO:-0} * 100" | bc)"
    printf "  Storage Savings:      %.2f%%\n" "$(echo "scale=2; ${SAVINGS:-0} * 100" | bc)"
    echo

    echo " Space Usage"
    echo "────────────────────────────────────────────────────────"
    printf "  Logical Size:         %s\n" "$(format_bytes ${LOGICAL:-0})"
    printf "  Actual Storage:       %s\n" "$(format_bytes ${STORAGE:-0})"
    printf "  Bytes Saved:          %s\n" "$(format_bytes ${SAVED:-0})"
    echo

    if [ -n "$DEDUP_HITS" ] || [ -n "$DEDUP_MISSES" ]; then
        echo " Deduplication"
        echo "────────────────────────────────────────────────────────"
        printf "  Hits (reused):        %s\n" "${DEDUP_HITS:-0}"
        printf "  Misses (new):         %s\n" "${DEDUP_MISSES:-0}"
        echo
    fi

    # HTTP Request Duration Histogram
    echo " HTTP Request Timing"
    echo "────────────────────────────────────────────────────────"

    # Parse histogram data for each method/endpoint combination
    for method in GET PUT DELETE; do
        for endpoint in "/ft/files" "/ft/list" "/ft/version"; do
            COUNT=$(echo "$METRICS" | grep "s3dedup_http_request_duration_seconds_count{endpoint=\"$endpoint\",method=\"$method\"}" | awk '{print $NF}')
            SUM=$(echo "$METRICS" | grep "s3dedup_http_request_duration_seconds_sum{endpoint=\"$endpoint\",method=\"$method\"}" | awk '{print $NF}')

            if [ -n "$COUNT" ] && [ "$COUNT" != "0" ]; then
                AVG=$(echo "scale=3; $SUM / $COUNT * 1000" | bc)
                printf "  %-6s %-12s %6s req, avg %7.2f ms\n" "$method" "$endpoint" "$COUNT" "$AVG"
            fi
        done
    done
    echo

    echo "️  Uptime:              $(format_uptime ${UPTIME:-0})"
    echo
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Press Ctrl+C to exit  |  Refreshing every 5 seconds..."
    echo "  Metrics URL: $METRICS_URL"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    sleep 5
done
