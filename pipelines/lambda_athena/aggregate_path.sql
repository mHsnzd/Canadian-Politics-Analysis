 SELECT 
            year,
            month,
            day,
            label,
            sentiment,
            emotion,
            hate_speech,
            AVG(sentiment_score) AS average_sentiment_score,
            COUNT(sentiment) AS comment_count,
            AVG(score) AS average_upvotes
        FROM 
            reddit.nlp
        WHERE "$path" NOT LIKE 's3://cmpt732/transformed/reddit_submissions%'
        GROUP BY 
            year, 
            month,
            day, 
            label, 
            sentiment, 
            emotion, 
            hate_speech
        ORDER BY 
            year, 
            month,
            day, 
            label, 
            sentiment, 
            emotion, 
            hate_speech;
