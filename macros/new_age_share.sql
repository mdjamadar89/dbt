{% macro new_age_share() %}

    {% set learn_course_progress %}
        CREATE OR REPLACE SECURE VIEW newage.learn_course_progress AS
            SELECT * FROM reports.learn_course_progress
            WHERE site_id =  '110'
    {% endset %}
    {% do run_query(learn_course_progress) %}
    {% set grant_learn_course_progress %}
        GRANT SELECT ON VIEW newage.learn_course_progress TO SHARE newage
    {% endset %}
    {% do run_query(grant_learn_course_progress) %}


    {% set learn_lesson_progress %}
        CREATE OR REPLACE SECURE VIEW newage.learn_lesson_progress AS
            SELECT * FROM reports.learn_lesson_progress
            WHERE site_id =  '110'
    {% endset %}
    {% do run_query(learn_lesson_progress) %}
    {% set grant_learn_lesson_progress %}
        GRANT SELECT ON VIEW newage.learn_lesson_progress TO SHARE newage
    {% endset %}
    {% do run_query(grant_learn_lesson_progress) %}


    {% set learn_path_progress %}
        CREATE OR REPLACE SECURE VIEW newage.learn_path_progress AS
            SELECT * FROM reports.learn_path_progress
            WHERE site_id =  '110'
    {% endset %}
    {% do run_query(learn_path_progress) %}
    {% set grant_learn_path_progress %}
        GRANT SELECT ON VIEW newage.learn_path_progress TO SHARE newage
    {% endset %}
    {% do run_query(grant_learn_path_progress) %}


    {% set learn_user_quiz_answers %}
        CREATE OR REPLACE SECURE VIEW newage.learn_user_quiz_answers AS
            SELECT * FROM reports.learn_user_quiz_answers
            WHERE site_id =  '110'
    {% endset %}
    {% do run_query(learn_user_quiz_answers) %}
    {% set grant_learn_user_quiz_answers %}
        GRANT SELECT ON VIEW newage.learn_user_quiz_answers TO SHARE newage
    {% endset %}
    {% do run_query(grant_learn_user_quiz_answers) %}


    {% set learn_course_progress %}
        CREATE OR REPLACE SECURE VIEW newage.learn_course_progress AS
            SELECT * FROM reports.learn_course_progress
            WHERE site_id =  '110'
    {% endset %}
    {% do run_query(learn_course_progress) %}
    {% set grant_learn_course_progress %}
        GRANT SELECT ON VIEW newage.learn_course_progress TO SHARE newage
    {% endset %}
    {% do run_query(grant_learn_course_progress) %}

{% endmacro %}