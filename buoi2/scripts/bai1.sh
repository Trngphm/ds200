#!/bin/bash

set -e

# ================================
# CONFIG
# ================================
HDFS_OUTPUT="/user/trang/output/bai1"
LOCAL_OUTPUT_DIR="output"

# ================================
# XOÁ OUTPUT CŨ
# ================================
echo "🗑️ Cleaning old output..."
hdfs dfs -rm -r -f $HDFS_OUTPUT || true
rm -r "output/bai1" || true
rm pig_*.log || true

# ================================
# CHẠY PIG
# ================================
echo "🚀 Running Pig..."
pig bai1.pig

# ================================
# TẠO THƯ MỤC LOCAL
# ================================
mkdir -p $LOCAL_OUTPUT_DIR

# ================================
# TẢI FILE VỀ LOCAL
# ================================
echo "⬇️ Downloading result to local..."
hdfs dfs -get $HDFS_OUTPUT $LOCAL_OUTPUT_DIR/
