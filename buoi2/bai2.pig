-- LOAD DATA
data = LOAD 'output/bai1' USING PigStorage(',') AS (
    id: int,
    word: chararray
);

review_data = LOAD 'datasets/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

stopwords = LOAD 'datasets/stopwords.txt' USING PigStorage('\n') 
AS (word: chararray);



-- thống kê tần soố từ
group_word = GROUP data BY word;
count_word = FOREACH group_word GENERATE 
    group AS word, 
    COUNT(data) AS count;

-- lấy từ xh trên 500 lần
word_over_500 = FILTER count_word BY count > 500;

DUMP word_over_500;

-- thống kê số bình luận theo category
distinct_category = DISTINCT (FOREACH review_data GENERATE id, category);
group_category = GROUP distinct_category BY category;

count_category = FOREACH group_category GENERATE 
    group AS category, 
    COUNT(distinct_category) AS count;

DUMP count_category;

-- thống kê số bình luận theo aspect
distinct_aspect = DISTINCT (FOREACH review_data GENERATE id, aspect);
group_aspect = GROUP distinct_aspect BY aspect;

count_aspect = FOREACH group_aspect GENERATE 
    group AS aspect, 
    COUNT(distinct_aspect) AS count;

DUMP count_aspect;

STORE word_over_500 INTO '/user/trang/output/bai2/word_freq' USING PigStorage(',');
STORE count_category INTO '/user/trang/output/bai2/category_count' USING PigStorage(',');
STORE count_aspect INTO '/user/trang/output/bai2/aspect_count' USING PigStorage(',');