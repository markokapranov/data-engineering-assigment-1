WITH init as (
SELECT json_extract(titleText,'$.text') as title,
    json_extract(titleType,'$.text') as type,
    CAST(json_extract(releaseYear,'$.year') AS INT) as year_release,
    CAST(json_extract(runtime,'$.seconds') AS INT) as runtime,
    CAST(json_extract(ratingsSummary,'$.aggregateRating') AS DECIMAL(5,1)) as rating,
    CAST(json_extract(metacritic,'$.metascore.score') AS INT) as meta_score,
    json_extract(certificate,'$.rating') as certificate,
    json_extract(countriesOFOrigin,'$.countries')::VARCHAR[] as countries,
    json_extract(genres,'$.genres')::VARCHAR[] as genres
    FROM read_json_auto('movies_batch_1.json')
    WHERE json_extract(ratingsSummary,'$.aggregateRating') != 'null'
    and json_extract(metacritic,'$.metascore.score') != 'null'
    ),
    processed as (
    SELECT title,
           type,
           year_release,
           runtime,
           rating,
           meta_score,
           certificate,
           json_extract(country_json, '$.text') as country,
           json_extract(genre_json, '$.text') as genre
    from init, unnest(countries) as t(country_json), unnest(genres) as t(genre_json)
    ),
    deleting_dups as (
    SELECT title,
           country,
           meta_score,
           FROM processed
           GROUP BY ALL
    ),
    window_func as (
    SELECT *,
    ROW_NUMBER() OVER ( PARTITION BY country ORDER BY meta_score DESC) as rank
    FROM deleting_dups
    )
    SELECT *
    FROM window_func
    WHERE rank <= 3
    ORDER BY country;


WITH init as (
    SELECT json_extract(titleText,'$.text') as title,
           json_extract(titleType,'$.text') as type,
           CAST(json_extract(releaseYear,'$.year') AS INT) as year_release,
           CAST(json_extract(runtime,'$.seconds') AS INT) as runtime,
           CAST(json_extract(ratingsSummary,'$.aggregateRating') AS DECIMAL(5,1)) as rating,
           CAST(json_extract(metacritic,'$.metascore.score') AS INT) as meta_score,
           json_extract(certificate,'$.rating') as certificate,
           json_extract(countriesOFOrigin,'$.countries')::VARCHAR[] as countries,
        json_extract(genres,'$.genres')::VARCHAR[] as genres
    FROM read_json_auto('movies_batch_1.json')
    WHERE json_extract(ratingsSummary,'$.aggregateRating') != 'null'
    and json_extract(metacritic,'$.metascore.score') != 'null'
    ),
    processed as (
SELECT title,
    type,
    year_release,
    runtime,
    rating,
    meta_score,
    certificate,
    json_extract(country_json, '$.text') as country,
    json_extract(genre_json, '$.text') as genre
from init, unnest(countries) as t(country_json), unnest(genres) as t(genre_json)
    ),
    deleting_dups as (
SELECT
    country,
    genre,
    CAST(AVG(rating) AS DECIMAL(4,1)) as avg_score
FROM processed
GROUP BY genre, country
    ),
    window_func as (
SELECT *,
    ROW_NUMBER() OVER ( PARTITION BY country ORDER BY avg_score DESC) as rank
FROM deleting_dups
    )
SELECT *
FROM window_func
WHERE rank <= 5
ORDER BY country, rank;