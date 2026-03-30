-- ================================
-- LOAD DATA
-- ================================

words = LOAD 'output/bai1' USING PigStorage(',') AS (
    id: int,
    word: chararray
);

reviews = LOAD 'datasets/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- ================================
-- JOIN
-- ================================
joined = JOIN words BY id, reviews BY id;

data = FOREACH joined GENERATE 
    reviews::category AS category,
    reviews::sentiment AS sentiment,
    words::word AS word;

-- ================================
-- POSITIVE
-- ================================
pos = FILTER data BY sentiment == 'positive';

group_pos = GROUP pos BY (category, word);

count_pos = FOREACH group_pos GENERATE 
    FLATTEN(group) AS (category, word),
    COUNT(pos) AS freq;

group_pos2 = GROUP count_pos BY category;

top5_pos = FOREACH group_pos2 {
    sorted = ORDER count_pos BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE group AS category, top5;
};

DUMP top5_pos;

-- ================================
-- NEGATIVE
-- ================================
neg = FILTER data BY sentiment == 'negative';

group_neg = GROUP neg BY (category, word);

count_neg = FOREACH group_neg GENERATE 
    FLATTEN(group) AS (category, word),
    COUNT(neg) AS freq;

group_neg2 = GROUP count_neg BY category;

top5_neg = FOREACH group_neg2 {
    sorted = ORDER count_neg BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE group AS category, top5;
};

DUMP top5_neg;

-- store 
STORE top5_pos INTO '/user/trang/output/bai4/top5_positive' USING PigStorage(',');
STORE top5_neg INTO '/user/trang/output/bai4/top5_negative' USING PigStorage(',');