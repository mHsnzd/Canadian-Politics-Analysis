SELECT 
    year,
    month,
    label,
    sentiment,
    emotion,
    hate_speech,
    AVG(sentiment_score) AS average_sentiment_score,
    COUNT(sentiment) AS comment_count,
    AVG(score) AS average_upvotes
FROM 
    reddit.nlp
GROUP BY 
    year, 
    month, 
    label, 
    sentiment, 
    emotion, 
    hate_speech
ORDER BY 
    year, 
    month, 
    label, 
    sentiment, 
    emotion, 
    hate_speech;
