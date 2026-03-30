data = LOAD 'datasets/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
    );

-- stopwords 
stopwords = LOAD 'datasets/stopwords.txt' USING PigStorage('\n') 
AS (word: chararray);

-- lower case
lowercased = FOREACH data GENERATE 
    id, 
    LOWER(review) AS review;

-- remove punctuation
cleaned = FOREACH lowercased GENERATE 
    id, 
    REPLACE(
        REPLACE(review, '\\b\\p{N}+\\p{L}*\\b', ''),   -- xoá số + đơn vị
        '[^\\p{L}\\s]', ''                             -- xoá punctuation
    ) AS review;

-- tokenize
tokenized = FOREACH cleaned GENERATE 
    id, 
    FLATTEN(TOKENIZE(review)) AS word;

-- remove stopwords
joined = JOIN tokenized BY word LEFT OUTER, stopwords BY word;

filtered = FILTER joined BY stopwords::word IS NULL;

-- Kết quả cuối
result = FOREACH filtered GENERATE tokenized::id, tokenized::word;

DUMP result;

STORE result INTO '/user/trang/output/bai1'
USING PigStorage(',');