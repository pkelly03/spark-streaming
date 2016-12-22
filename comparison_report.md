Performed the following search on both python and scala

userId : rudzud
sessionId : rudzud#3587
seedItemId : 3587
targetItemId : 30965


explanation_id - match
user_id - match
session_id - match
target_item_id - match
target_item_mentions - match
target_item_sentiment - match
better_count - match
worse_count - match
better_pro_scores - more or less match (python version contained a few extra digits, python - 0.6666666, scala 0.6)
worse_cons_scores - more or less match (python version contained a few extra digits, python - 0.6666666, scala 0.6)
is_seed = match
pros - match
cons - match
n_pros - match
n_cons - match
strength - mismatch, python 3, scala 2.6999999997
betterAverage - mismatch - scala - 0.674999, python 0.75
worseAverage - match
better_average_comp - mismatch - python 0.75, scala 0.6749999
worse_average_comp - match - but false positive probably  - it was a 0
strength_comp - mismatch - python 3, scala 2.69999


Python below

{
    "target_item_sentiment": [
        0.8769230769230769,
        0.8,
        0.9076923076923077,
        0.8461538461538461
    ],
    "is_seed": false,
    "worse_avg_comp": 0.0,
    "target_item_mentions": [
        0.25,
        0.25,
        0.25,
        0.25
    ],
    "cons": [
        false,
        false,
        false,
        false
    ],
    "cons_comp": [
        false,
        false,
        false,
        false
    ],
    "worse_count": [
        3,
        3,
        1,
        2
    ],
    "worse_con_scores": [
        0.3333333333333333,
        0.3333333333333333,
        0.1111111111111111,
        0.2222222222222222
    ],
    "strength_comp": 3.0,
    "strength": 3.0,
    "user_id": "rudzud",
    "seed_item_id": "3587",
    "better_count": [
        6,
        6,
        8,
        7
    ],
    "rec_sim": 0.9998999650487166,
    "pros": [
        true,
        true,
        true,
        true
    ],
    "n_cons_comp": 0,
    "better_avg": 0.75,
    "target_item_average_rating": 4.153846153846154,
    "target_item_star": 4.153846153846154,
    "pros_comp": [
        true,
        true,
        true,
        true
    ],
    "target_item_id": "u'30965'",
    "better_avg_comp": 0.75,
    "rank_average_rating": 4,
    "average_rating": 4.153846153846154,
    "n_pros": 4,
    "n_pros_comp": 4,
    "explanation_id": "rudzud#3587##30965",
    "n_cons": 0,
    "is_comp": true,
    "rank_target_item_star": 4,
    "better_pro_scores": [
        0.6666666666666666,
        0.6666666666666666,
        0.8888888888888888,
        0.7777777777777778
    ],
    "session_id": "rudzud#3587",
    "rank_strength": 2,
    "worse_avg": 0.0,
    "rank_target_item_average_rating": 4,
    "rank_rec_sim": 5,
    "rank_strength_comp": 2
}