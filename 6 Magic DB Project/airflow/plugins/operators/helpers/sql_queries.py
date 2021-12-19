class SqlQueries:
    """
    Auxiliar SQL queries to create the fact and dimension tables of the DB.
    """

    cards_table_insert = ("""
        SELECT DISTINCT id as card_id
             , name as card_name
             , lang as card_lang
             , released_at
             , layout
             , mana_cost
             , cmc
             , type_line
             , oracle_text
             , power
             , toughness
             , colors
             , color_identity
             , keywords
             , standard_legal
             , pioneer_legal
             , modern_legal
             , legacy_legal
             , historic_legal
             , reserved
             , foil
             , nonfoil
             , oversized
             , promo
             , reprint
             , variation
             , set_id
             , collector_number
             , digital
             , rarity
             , artist_ids
             , border_color
             , frame
             , full_art
             , textless
             , booster
             , story_spotlight
             , printed_name
             , printed_type_line
             , printed_text
             , security_stamp
             , loyalty
             , watermark
             , produced_mana
             , color_indicator
             , content_warning
             , life_modifier
             , hand_modifier
        FROM staging_scryfall
    """)

    sets_table_insert = ("""
        SELECT DISTINCT set_id
             , set
             , set_name
             , set_type
        FROM staging_scryfall
    """)

    artists_table_insert = ("""
        WITH NS AS (
            SELECT 1 as n UNION ALL
            SELECT 2 UNION ALL
            SELECT 3
        )
        SELECT DISTINCT TRIM(SPLIT_PART(REPLACE(REPLACE(REPLACE(B.artist_ids, '[', ''), ']', ''), '"', ''), ',', NS.n)) AS artist_id
             , TRIM(SPLIT_PART(B.artist, '&', NS.n)) as artist_name
        FROM NS
        INNER JOIN (
            SELECT artist
                 , artist_ids
            FROM staging_scryfall
        ) B ON NS.n <= json_array_length(B.artist_ids, true)
    """)

    time_table_insert = ("""
        SELECT DISTINCT dt
             , extract(day from dt) as day
             , extract(month from dt) as month
             , extract(year from dt) as year
        FROM staging_prices
    """)

    prices_table_insert = ("""
        SELECT DISTINCT md5(cards.id || prices.online_paper || prices.store || prices.price_type || prices.card_type || dt) prices_id
             , cards.id as card_id
             , prices.online_paper
             , prices.store
             , prices.price_type
             , prices.card_type
             , prices.currency
             , prices.dt
             , prices.price
        FROM staging_prices prices
        JOIN staging_prints prints ON prices.card_id = prints.card_id
        JOIN staging_scryfall cards ON trim(lower(prints.name)) = trim(lower(cards.name))
                                       AND prints.collector_number = cards.collector_number
                                       AND trim(lower(prints.edition)) = trim(lower(cards.set))
        WHERE prices.dt = '{date}'
    """)
