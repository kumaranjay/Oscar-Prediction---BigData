REGISTER '/Users/kumaran_jay/Documents/Bigdata_Final_Kumaran/MovieRatings_PigUDF.jar'
DEFINE BAYESIAN bayesian.bayesian;
ratings = LOAD '/Users/kumaran_jay/Documents/Bigdata_Final_Kumaran/ml-latest/ratings.csv'
USING PigStorage(',')
AS (userId:int, movieId:int, rating:double, timestamp:int);
select = FOREACH ratings GENERATE movieId, rating;
grp = GROUP select BY movieId;
selectTwo = FOREACH grp GENERATE group AS movieIdR, AVG(select.rating) AS avgRating, COUNT($1) AS reviewCount;
movieNames = LOAD '/Users/kumaran_jay/Documents/Bigdata_Final_Kumaran/ml-latest/movies.csv' USING PigStorage(',') AS (movieId:int,title:chararray,genres:chararray);
selectMovie = FOREACH movieNames GENERATE movieId, title;
joinT = JOIN selectTwo BY movieIdR, selectMovie by movieId;
final = FOREACH joinT GENERATE movieIdR, title, avgRating, reviewCount;
wrcalc = FOREACH final GENERATE movieIdR,title,FLATTEN(BAYESIAN(avgRating,reviewCount)) AS wr;
final2 = FOREACH wrcalc GENERATE movieIdR, title,wr;
sortedTwo = ORDER final2 BY wr DESC;
STORE sortedTwo INTO '/Users/kumaran_jay/Documents/Bigdata_Final_Kumaran/AVGratings';