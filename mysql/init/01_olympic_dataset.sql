USE olympic_dataset;

CREATE TABLE IF NOT EXISTS athlete_event_results (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    athlete_name VARCHAR(100) NOT NULL,
    event_name VARCHAR(150) NOT NULL,
    medal VARCHAR(10) NULL,
    PRIMARY KEY (id),
    INDEX idx_athlete_event_results_medal (medal)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO athlete_event_results (athlete_name, event_name, medal) VALUES
    ('Athlete 01', '100 metres', 'Bronze'),
    ('Athlete 02', 'Long jump', 'Bronze'),
    ('Athlete 03', 'Swimming 200 metres', 'Bronze'),
    ('Athlete 04', 'Cycling road race', 'Bronze'),
    ('Athlete 05', 'High jump', 'Silver'),
    ('Athlete 06', 'Rowing single sculls', 'Silver'),
    ('Athlete 07', 'Boxing lightweight', 'Silver'),
    ('Athlete 08', 'Marathon', 'Gold'),
    ('Athlete 09', 'Pole vault', 'Gold'),
    ('Athlete 10', 'Javelin throw', 'Gold'),
    ('Athlete 11', 'Gymnastics all-around', 'Gold'),
    ('Athlete 12', 'Tennis singles', 'Gold'),
    ('Athlete 13', 'Archery individual', NULL),
    ('Athlete 14', 'Sailing', NULL);
