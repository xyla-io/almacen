--
-- view
--
CREATE OR REPLACE VIEW {SCHEMA}last_task_completion AS
  SELECT task_identifier, MAX(modification_time) AS last_completed, MAX(target_end_time) AS data_until
  FROM {SCHEMA}track_task_history
  WHERE status = 'completed'
    AND DATEDIFF(day, CURRENT_DATE, modification_time) <= 30
  GROUP BY task_identifier
  ORDER BY task_identifier, last_completed