## Core Entities and Key Fields

### Primary Entities:
1. **Players**:
   - Primary keys: `player_id`, `PERSON_ID`, or `personId` (varies by table)
   - Tables: `raw.players`, `raw.player_index`, `raw.common_player_info_common_player_info`

2. **Teams**:
   - Primary keys: `team_id`, `TEAM_ID`, or `teamId` (varies by table)
   - Tables: `raw.teams`, `raw.team_info`

3. **Games**:
   - Primary keys: `game_id`, `GAME_ID`, or `gameId` (varies by table)
   - Tables: `raw.season_games`, various boxscore and game summary tables

4. **Seasons**:
   - Referenced in multiple tables as `season_year`, `seasonYear`, or `SEASON`

### Key Relationships (implied by composite primary keys and indexes):

1. **Player-Game Relationships**:
   - Player boxscores: Composite keys of (gameId, personId)
   - Player game logs: Composite keys of (GAME_ID, PLAYER_ID)
   - Tracking the performance of players in specific games

2. **Team-Game Relationships**:
   - Team boxscores: Composite keys of (gameId, teamId)
   - Team game logs: Composite keys of (GAME_ID, TEAM_ID)
   - Representing team participation and performance in games

3. **Player-Team Relationships**:
   - Implied through boxscores where both player and team IDs appear
   - Represents which players played for which teams

4. **Player Matchups**:
   - `player_boxscore_matchups_v3`: (gameId, personIdOff, personIdDef)
   - Represents player vs player defensive matchups

5. **Game Context**:
   - Home/away team relationships in `int_game__schedules` and similar tables
   - Includes information about game location, season, etc.

6. **Key Composite Identifiers**:
   - `player_game_key`: Appears in many tables, unique identifier for a player's performance in a specific game
   - `team_game_key`: Unique identifier for a team's performance in a specific game
   - `player_matchup_key`: Unique identifier for matchups between players

## Neo4j Schema Recommendations:

### Nodes:
1. **Player** nodes:
   - Primary identifier: `player_id`
   - Properties: name, position, height, weight, country, etc.

2. **Team** nodes:
   - Primary identifier: `team_id`
   - Properties: name, city, conference, division, etc.

3. **Game** nodes:
   - Primary identifier: `game_id`
   - Properties: date, season, home_score, away_score, etc.

4. **Season** nodes:
   - Primary identifier: `season_year`
   - Properties: start_date, end_date, etc.

5. **Venue** nodes:
   - Properties: name, city, capacity

### Relationships:
1. **PLAYED_IN**: Player to Game relationship
   - Properties: minutes, points, rebounds, assists, etc.

2. **PARTICIPATED_IN**: Team to Game relationship
   - Properties: points, rebounds, win/loss, etc.

3. **PLAYED_FOR**: Player to Team relationship
   - Properties: season, start_date, end_date, etc.

4. **MATCHED_AGAINST**: Player to Player relationship within Game context
   - Properties: defensive stats, possessions, points allowed, etc.

5. **HOME_TEAM/AWAY_TEAM**: Team to Game relationships
   - Different relationship types for home vs away

6. **PART_OF**: Game to Season relationship

7. **HOSTED_AT**: Game to Venue relationship

The database schema already has several tables preparing data for Neo4j exports in the `intermediate.int_neo4j_export__*` tables, which include node and relationship definitions such as:
- Nodes: players, teams, games, seasons, venues
- Relationships: attended, played_for, played_in, from_country, etc.

These tables likely contain the exact mappings you'll need for your Neo4j schema, already prepared in a format ready for import.
