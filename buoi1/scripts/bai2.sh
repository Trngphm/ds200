#!/bin/bash

echo "--- START Hadoop Job BAI2 ---"

# 1. Xóa output cũ
hdfs dfs -rm -r -f output

# 2. Xóa dataset cũ
hdfs dfs -rm -r -f /user/trang/datasets

# 3. Compile + jar
javac -classpath $(hadoop classpath) -d . bai2.java
jar -cvf bai2.jar bai2

# 4. Upload dữ liệu
hdfs dfs -mkdir -p /user/trang/datasets

hdfs dfs -put datasets/ratings_1.txt /user/trang/datasets/
hdfs dfs -put datasets/ratings_2.txt /user/trang/datasets/
hdfs dfs -put datasets/movies.txt /user/trang/datasets/

# 5. Run job
hadoop jar bai2.jar bai2.bai2 \
/user/trang/datasets/ratings_1.txt \
/user/trang/datasets/ratings_2.txt \
/user/trang/datasets/movies.txt \
output

# 6. Check output
echo "--- OUTPUT FILES ---"
hdfs dfs -ls output

echo "--- RESULT (preview) ---"
hdfs dfs -cat output/part-r-00000 | head -n 10

# 7. Lấy file kết quả về local
echo "--- COPY RESULT TO LOCAL ---"
rm -f output/output_bai2
hdfs dfs -get output/part-r-00000 output/output_bai2

echo "--- DONE ---"