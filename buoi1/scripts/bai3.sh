#!/bin/bash

echo "--- START Hadoop Job BAI3 ---"

# 1. Xóa output cũ
hdfs dfs -rm -r -f output
hdfs dfs -rm -r -f temp_output

# 2. Xóa dataset cũ
hdfs dfs -rm -r -f /user/trang/datasets

# 3. Compile + jar
javac -classpath $(hadoop classpath) -d . bai3.java
jar -cvf bai3.jar bai3

# 4. Upload dữ liệu
hdfs dfs -mkdir -p /user/trang/datasets

hdfs dfs -put datasets/ratings_1.txt /user/trang/datasets/
hdfs dfs -put datasets/ratings_2.txt /user/trang/datasets/
hdfs dfs -put datasets/users.txt /user/trang/datasets/
hdfs dfs -put datasets/movies.txt /user/trang/datasets/

hdfs dfs -cat /user/trang/datasets/ratings_1.txt /user/trang/datasets/ratings_2.txt | hdfs dfs -put - /user/trang/datasets/ratings.txt

# 5. Run job
hadoop jar bai3.jar bai3.bai3 \
/user/trang/datasets/ratings.txt \
/user/trang/datasets/users.txt \
/user/trang/datasets/movies.txt \
output

# 6. Check output
echo "--- OUTPUT FILES ---"
hdfs dfs -ls output

echo "--- RESULT (preview) ---"
hdfs dfs -cat output/part-r-00000 | head -n 10

# 7. Lấy file kết quả về local (đúng yêu cầu bạn)
echo "--- COPY RESULT TO LOCAL ---"
rm -f output/output_bai3
hdfs dfs -get output/part-r-00000 output/output_bai3
echo "--- DONE ---"