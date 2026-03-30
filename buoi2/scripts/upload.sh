#!/bin/bash

set -e

# ================================
# CONFIG
# ================================
LOCAL_DATASET_DIR="datasets"
HDFS_DIR="/user/trang/datasets"

# ================================
# TẠO THƯ MỤC HDFS
# ================================
echo "📁 Creating HDFS directory..."
hdfs dfs -mkdir -p $HDFS_DIR

# ================================
# UPLOAD FILE
# ================================
echo "⬆️ Uploading files to HDFS..."
hdfs dfs -put -f $LOCAL_DATASET_DIR/hotel-review.csv $HDFS_DIR/
hdfs dfs -put -f $LOCAL_DATASET_DIR/stopwords.txt $HDFS_DIR/

# ================================
# KIỂM TRA
# ================================
echo "📊 Checking HDFS directory..."
hdfs dfs -ls $HDFS_DIR

echo "✅ Upload done!"