-- LOAD
words = LOAD 'output/bai1' USING PigStorage(',') AS (
    id:int,
    word:chararray
);

reviews = LOAD 'datasets/hotel-review.csv' USING PigStorage(';') AS (
    id:int,
    review:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

-- JOIN
joined = JOIN words BY id, reviews BY id;

data = FOREACH joined GENERATE 
    reviews::category AS category,
    words::word AS word;

-- ================================
-- COUNT WORD THEO CATEGORY
-- ================================
grouped = GROUP data BY (category, word);

counted = FOREACH grouped GENERATE 
    FLATTEN(group) AS (category, word),
    COUNT(data) AS freq;

-- ================================
-- TOP 5 MỖI CATEGORY
-- ================================
group2 = GROUP counted BY category;

top5 = FOREACH group2 {
    sorted = ORDER counted BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE group AS category, top;
};

DUMP top5;

-- STORE (optional)
STORE top5 INTO '/user/trang/output/bai5/top5_words_by_category'
USING PigStorage(',');