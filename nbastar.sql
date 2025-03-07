INSERT INTO NBA24_25_S.NBATEAMS_S(TEAM_ID,
                                  FULL_NAME,
                                  ABBREVIATION,
                                  NICKNAME,
                                  YEAR_FOUNDED,
                                  TEAM_CITY,
                                  TEAM_STATE,
                                  TEAM_URL_LOGO)
    SELECT 
    N.TEAM_ID,
    N.FULL_NAME,
    N.ABBREVIATION,
    N.NICKNAME,
    N.YEAR_FOUNDED,
    C.TEAM_CITY,
    S.TEAM_STATE,
    N.TEAM_URL_LOGO 
    FROM NBA24_25.NBATEAMS N
    JOIN NBA24_25.NBA_TEAM_STATES S ON
    N.TEAM_ID=S.TEAM_ID
    JOIN NBA24_25.NBA_TEAM_CITY C ON
    S.TEAM_ID=C.TEAM_ID;

INSERT INTO NBA24_25_S.NBA_GAMES_S(GAME_ID,
                                   GAME_DATE,
                                   HOME_TEAM_ID,
                                   VISITING_TEAM_ID)
    SELECT 
    N.GAME_ID,
    N.GAME_DATE,
    HOME_TEAM_ID,
    VISITING_TEAM_ID
    FROM NBA24_25.NBA_GAMES N 
    JOIN NBA24_25.HOME_VISITING_TEAM HM
    ON N.GAME_ID=HM.GAME_ID;

INSERT INTO NBA24_25_S.ACTIVE_NBA_PLAYERS_S(PLAYER_ID,
                                            TEAM_ID,
                                            PLAYER_LAST_NAME,
                                            PLAYER_FIRST_NAME,
                                            JERSEY_NUMBER,
                                            POSITION,
                                            PLAYER_HEIGHT,
                                            PLAYER_WEIGHT,
                                            COLLEGE,
                                            COUNTRY,
                                            DRAFT_YEAR,
                                            DRAFT_ROUND,
                                            DRAFT_NUMBER,
                                            PTS,
                                            REB,
                                            AST,
                                            PLAYER_PICTURE_URL)
    SELECT 
    n.PLAYER_ID,
    n.TEAM_ID,
    n.PLAYER_LAST_NAME,
    n.PLAYER_FIRST_NAME,
    n.JERSEY_NUMBER,
    n.POSITION,
    n.PLAYER_HEIGHT,
    n.PLAYER_WEIGHT,
    n.COLLEGE,
    n.COUNTRY,
    n.DRAFT_YEAR,
    n.DRAFT_ROUND,
    n.DRAFT_NUMBER,
    n.PTS,
    n.REB,
    n.AST,
    n.PLAYER_PICTURE_URL
    FROM NBA24_25.ACTIVE_NBA_PLAYERS n;

INSERT INTO NBA24_25_S.NBA_GAMES_TEAM_STATS_S(GAME_ID,
                                              TEAM_ID,
                                              WL,
                                              MIN,
                                              FGM,
                                              FGA,
                                              FG_PCT,
                                              FG3M,
                                              FG3A,
                                              FG3_PCT,
                                              FTM,
                                              FTA,
                                              FT_PCT,
                                              OREB,
                                              DREB,
                                              REB,
                                              AST,
                                              STL,
                                              BLK,
                                              PF,
                                              PTS,
                                              PLUS_MINUS)
    SELECT 
    T.GAME_ID,
    T.TEAM_ID,
    T.WL,
    T.MIN,
    T.FGM,
    T.FGA,
    C.FG_PCT,
    T.FG3M,
    T.FG3A,
    C.FG3_PCT,
    T.FTM,
    T.FTA,
    FT_PCT,
    T.OREB,
    T.DREB,
    C.REB,
    T.AST,
    T.STL,
    T.BLK,
    T.PF,
    C.PTS,
    T.PLUS_MINUS
    FROM NBA24_25.NBA_GAMES_TEAM_STATS T
    LEFT JOIN NBA24_25.NBA_GAMES_TEAM_STATS_COMPUTED C
    ON T.GAME_ID=C.GAME_ID AND T.TEAM_ID=C.TEAM_ID
