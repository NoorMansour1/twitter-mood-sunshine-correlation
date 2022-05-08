# Exploring the effect of sunshine radiation on twitter sentiment in the Netherlands
 
The weather can have an impact on people's mood. Social media platforms like Twitter are a big source of data when it comes to the sentiment of the population. This project aims to identify what the influence of time and solar radiation is on the sentiment of Dutch tweets. Sentiment analysis was performed on a dataset of Dutch tweets that are grouped per hour of the day. These sentiment scores were compared to the solar radiation at that specific time. With regards to time it was found that sentiment of Dutch tweets is higher in the evening compared to daytime. Furthermore, it was found that solar radiation has a small, but significant positive correlation with sentiment of Dutch tweets.

The tweet data was processed using Apache PySpark on a HDFS cluster as a university project. The dutch tweet data belongs to the big data class with a 1.6 TB size. The weather and sunshine radiation data was retrieved from the Koninklijk Nederlands Meteorologisch Instituut (2018). 

| Correlation  | Aggregated Sentiment |
| ------------- | ------------- |
|![correlation](/visualization/correlation.png)  |  <img src="/visualization/Aggregated_Sentiment.png" alt="drawing" width="3000"/> |




