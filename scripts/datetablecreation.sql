
CREATE TABLE user_article (
  id INT not null auto_increment PRIMARY KEY,
  userId VARCHAR(50),
  articleId VARCHAR(50),
  category VARCHAR(50),
  added  BIGINT,
  updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO user_article (userId, articleId, category, added) VALUES ('0', '0', 'None', 0)

SELECT Count(*) FROM user_article
SELECT Count(*) FROM user_article_jb
DELETE FROM user_article 
INSERT INTO user_article (userId, articleId, category, added) VALUES ('71562563', '434255', 'None', 0);

CREATE TABLE user_article_jb (
  id INT not null auto_increment PRIMARY KEY,
  userId VARCHAR(50),
  articleId VARCHAR(50),
  category VARCHAR(50),
  added  BIGINT,
  updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE EVENT AUTO_CLEANUP_PK
ON SCHEDULE EVERY 1 DAY
ON COMPLETION PRESERVE
DO 
DELETE LOW_PRIORITY FROM pkuserarticles.user_article WHERE updated < DATE_SUB(NOW(), INTERVAL 2 DAY)



CREATE EVENT AUTO_CLEANUP_JB
ON SCHEDULE EVERY 1 DAY
ON COMPLETION PRESERVE
DO 
DELETE LOW_PRIORITY FROM pkuserarticles.user_article_jb WHERE updated < DATE_SUB(NOW(), INTERVAL 2 DAY)

ALTER TABLE user_article DROP COLUMN id;
ALTER TABLE user_article ADD PRIMARY KEY(updated);

ALTER TABLE user_article_jb DROP COLUMN id;
ALTER TABLE user_article_jb DROP PRIMARY KEY, ADD PRIMARY KEY(updated);
DROP EVENT AUTO_CLEANUP_JB

DROP EVENT AUTO_CLEANUP
SELECT * FROM mysql.event
