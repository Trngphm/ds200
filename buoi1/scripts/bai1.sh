#!/bin/bash

echo "--- START Hadoop Job BAI1 ---"

# 1. Xóa output cũ
hdfs dfs -rm -r -f output

# 2. Xóa dataset cũ trên HDFS
hdfs dfs -rm -r -f /user/trang/datasets

# 3. Compile + tạo jar
javac -classpath $(hadoop classpath) -d . bai1.java
jar -cvf bai1.jar bai1

# 4. Upload dữ liệu
hdfs dfs -mkdir -p /user/trang/datasets

hdfs dfs -put datasets/ratings_1.txt /user/trang/datasets/
hdfs dfs -put datasets/ratings_2.txt /user/trang/datasets/
hdfs dfs -put datasets/movies.txt /user/trang/datasets/

# 5. Run job
hadoop jar bai1.jar bai1.bai1 \
/user/trang/datasets/ratings_1.txt \
/user/trang/datasets/ratings_2.txt \
/user/trang/datasets/movies.txt \
output

# 6. Check output
echo "--- OUTPUT FILES ---"
hdfs dfs -ls output

echo "--- RESULT (preview) ---"
hdfs dfs -cat output/part-r-00000 | head -n 10

# 7. Copy về local
echo "--- COPY RESULT TO LOCAL ---"
rm -f output/output_bai1
hdfs dfs -get output/part-r-00000 output/output_bai1
echo "--- DONE ---"