/*
Auxiliar SQL DROP and CREATE TABLE statements for the Sparkify DB.
*/

DROP TABLE IF EXISTS public.staging_scryfall;
DROP TABLE IF EXISTS public.staging_prices;
DROP TABLE IF EXISTS public.staging_prints;

DROP TABLE IF EXISTS public."sets" cascade;
DROP TABLE IF EXISTS public.artists cascade;
DROP TABLE IF EXISTS public.time cascade;
DROP TABLE IF EXISTS public.cards cascade;
DROP TABLE IF EXISTS public.prices cascade;

CREATE TABLE IF NOT EXISTS public.staging_scryfall (
    id                  VARCHAR(36)
  , name                VARCHAR
  , lang                VARCHAR(10)
  , released_at         TIMESTAMP
  , layout              VARCHAR(50)
  , mana_cost           VARCHAR
  , cmc                 INT
  , type_line           VARCHAR
  , oracle_text         VARCHAR
  , power               VARCHAR(10)
  , toughness           VARCHAR(10)
  , colors              VARCHAR
  , color_identity      VARCHAR
  , keywords            VARCHAR
  , standard_legal      VARCHAR(20)
  , pioneer_legal       VARCHAR(20)
  , modern_legal        VARCHAR(20)
  , legacy_legal        VARCHAR(20)
  , historic_legal      VARCHAR(20)
  , reserved            BOOLEAN
  , foil                BOOLEAN
  , nonfoil             BOOLEAN
  , oversized           BOOLEAN
  , promo               BOOLEAN
  , reprint             BOOLEAN
  , variation           BOOLEAN
  , set_id              VARCHAR(36)
  , set                 VARCHAR(20)
  , set_name            VARCHAR
  , set_type            VARCHAR
  , collector_number    VARCHAR(20)
  , digital             BOOLEAN
  , rarity              VARCHAR(20)
  , artist              VARCHAR
  , artist_ids          VARCHAR
  , border_color        VARCHAR(20)
  , frame               VARCHAR
  , full_art            BOOLEAN
  , textless            BOOLEAN
  , booster             BOOLEAN
  , story_spotlight     BOOLEAN
  , printed_name        VARCHAR
  , printed_type_line   VARCHAR
  , printed_text        VARCHAR
  , security_stamp      VARCHAR(20)
  , loyalty             VARCHAR(10)
  , watermark           VARCHAR(20)
  , produced_mana       VARCHAR
  , color_indicator     VARCHAR
  , tcgplayer_etched_id INT
  , content_warning     INT
  , life_modifier       INT
  , hand_modifier       INT
);

CREATE TABLE IF NOT EXISTS public.staging_prices (
    card_id      VARCHAR(36)
  , online_paper VARCHAR(20)
  , store        VARCHAR(20)
  , price_type   VARCHAR(20)
  , card_type    VARCHAR(20)
  , dt           TIMESTAMP
  , price        FLOAT
  , currency     VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS public.staging_prints (
    card_id          VARCHAR(36)
  , name             VARCHAR
  , collector_number VARCHAR(20)
  , edition          VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS public."sets" (
    set_id   VARCHAR(36)
  , set      VARCHAR(20)
  , set_name VARCHAR
  , set_type VARCHAR
  , PRIMARY KEY (set_id)
);

CREATE TABLE IF NOT EXISTS public.artists (
    artist_id VARCHAR
  , artist    VARCHAR
  , PRIMARY KEY (artist_id)
);

CREATE TABLE IF NOT EXISTS public.time (
    dt    VARCHAR
  , day   INT
  , month INT
  , year  INT
  , PRIMARY KEY (dt)
);

CREATE TABLE IF NOT EXISTS public.cards (
    card_id           VARCHAR(36)
  , card_name         VARCHAR
  , card_lang         VARCHAR(10)
  , released_at       TIMESTAMP
  , layout            VARCHAR(50)
  , mana_cost         VARCHAR
  , cmc               INT
  , type_line         VARCHAR
  , oracle_text       VARCHAR
  , power             VARCHAR(10)
  , toughness         VARCHAR(10)
  , colors            VARCHAR
  , color_identity    VARCHAR
  , keywords          VARCHAR
  , standard_legal    VARCHAR(20)
  , pioneer_legal     VARCHAR(20)
  , modern_legal      VARCHAR(20)
  , legacy_legal      VARCHAR(20)
  , historic_legal    VARCHAR(20)
  , reserved          BOOLEAN
  , foil              BOOLEAN
  , nonfoil           BOOLEAN
  , oversized         BOOLEAN
  , promo             BOOLEAN
  , reprint           BOOLEAN
  , variation         BOOLEAN
  , set_id            VARCHAR(36)
  , collector_number  VARCHAR(20)
  , digital           BOOLEAN
  , rarity            VARCHAR(20)
  , artist_ids        VARCHAR
  , border_color      VARCHAR(20)
  , frame             VARCHAR
  , full_art          BOOLEAN
  , textless          BOOLEAN
  , booster           BOOLEAN
  , story_spotlight   BOOLEAN
  , printed_name      VARCHAR
  , printed_type_line VARCHAR
  , printed_text      VARCHAR
  , security_stamp    VARCHAR(20)
  , loyalty           VARCHAR(10)
  , watermark         VARCHAR(20)
  , produced_mana     VARCHAR
  , color_indicator   VARCHAR
  , content_warning   INT
  , life_modifier     INT
  , hand_modifier     INT
  , PRIMARY KEY (card_id)
  , FOREIGN KEY (set_id) REFERENCES "sets"(set_id)
  , FOREIGN KEY (artist_ids) REFERENCES artists(artist_id)
);

CREATE TABLE IF NOT EXISTS public.prices (
    prices_id     varchar(36)
  , card_id       VARCHAR(36)
  , online_paper  VARCHAR(20)
  , store         VARCHAR(20)
  , price_type    VARCHAR(20)
  , card_type     VARCHAR(20)
  , currency      VARCHAR(10)
  , dt            TIMESTAMP
  , price         FLOAT
  , PRIMARY KEY (prices_id)
  , FOREIGN KEY (card_id) REFERENCES cards(card_id)
  , FOREIGN KEY (dt) REFERENCES "time"(dt)
);