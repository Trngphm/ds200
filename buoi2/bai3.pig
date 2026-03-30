-- ================================
-- LOAD DATA
-- ================================
data = LOAD 'datasets/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- ================================
-- FILTER POSITIVE
-- ================================
positive = FILTER data BY sentiment == 'positive';

group_pos = GROUP positive BY aspect;

count_pos = FOREACH group_pos GENERATE 
    group AS aspect,
    COUNT(positive) AS total;

-- lấy aspect có positive nhiều nhất
sorted_pos = ORDER count_pos BY total DESC;
top_positive = LIMIT sorted_pos 1;

DUMP top_positive;

-- ================================
-- FILTER NEGATIVE
-- ================================
negative = FILTER data BY sentiment == 'negative';

group_neg = GROUP negative BY aspect;

count_neg = FOREACH group_neg GENERATE 
    group AS aspect,
    COUNT(negative) AS total;

-- lấy aspect có negative nhiều nhất
sorted_neg = ORDER count_neg BY total DESC;
top_negative = LIMIT sorted_neg 1;

DUMP top_negative;

-- ================================
-- STORE (optional)
-- ================================
STORE top_positive INTO '/user/trang/output/bai3/top_positive' USING PigStorage(',');
STORE top_negative INTO '/user/trang/output/bai3/top_negative' USING PigStorage(',');