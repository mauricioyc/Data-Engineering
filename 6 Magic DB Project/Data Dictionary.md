# Data Dictionary

## Fact Table
`prices`: fact table with card prices by date. The prices can be for online or paper MTG.
 - prices_id: price id.
 - card_id: card id.
 - online_paper: indicates if the price is for an online or paper card.
 - store: the store that originates the price.
 - price_type: indicates if the price is for retail or buylist (it is like a bid price a player can post in the store).
 - card_type: indicates if the card is foil or normal.
 - currency: the currency of the price.
 - dt: date the price was set.
 - price: price of the card.

## Dimension Table
`sets`: information about the set of the card. Each card belongs to only one set.
 - set_id: set uuid.
 - set: set abbreviation.
 - set_name: set name.
 - set_type: type of the set. Indicates for which format of play the set was created.

`artists`: artists that create the art of the card. The card can have more than one artist.
 - artist_id: artist uuid.
 - artist: artist name.

`time`: auxiliary time table.
 - dt: timestamp in the format YYYY-MM-DD.
 - day: day de dt.
 - month: month of dt.
 - year: year of dt.

`cards`: information of all cards in MTG. Each card is a single row, even if there is the same card in a different language.
 - card_id: card id.
 - card_name: card name in English, the official language.
 - card_lang: card printed language.
 - released_at: date that the card was released.
 - layout: card layout. Indicates the layout the card was printed.
 - mana_cost: mana cost of the card.
 - cmc: converted mana cost of the card.
 - type_line: type of the card in the official language.
 - oracle_text: text of the card in the official language.
 - power: power of the card. A card can have no power or a special character power.
 - toughness: toughness of the card. A card can have no toughness or a special character toughness.
 - colors: color of the card.
 - color_identity: color or colors of any mana symbol in the card.
 - keywords: card keywords in a JSON format.
 - standard_legal: indicates if the card is legal or not in Standard.
 - pioneer_legal: indicates if the card is legal or not in Pioneer.
 - modern_legal: indicates if the card is legal or not in Modern.
 - legacy_legal: indicates if the card is legal or not in Legacy.
 - historic_legal: indicates if the card is legal or not in Historic.
 - reserved: indicates if the card is reserved. A reserved card may not be reprinted to preserve its collector value.
 - foil: indicates if the card is foil.
 - nonfoil: indicates if the card is nonfoil.
 - oversized: indicates if the card is oversized.
 - promo: indicates if the card is promotional.
 - reprint: indicates if the card is a reprint.
 - variation: indicates if the card is a variation of the original card art.
 - set_id: set id.
 - collector_number: collector number of the card to identify the card series.
 - digital: indicates if the card is digital.
 - rarity: card rarity.
 - artist_ids: artist id.
 - border_color: border color of the card.
 - frame: frame type of the card.
 - full_art: indicates if the card is full art. Full art cards has no frame.
 - textless: indicates if the card is textless.
 - booster: indicates if the card can be acquired in a booster.
 - story_spotlight: indicates if the card is from a special group of Story Spotlight.
 - printed_name: printed name of the card. Usually is the name of the card in the language printed.
 - printed_type_line: type of the card in the language printed.
 - printed_text: text of the card in the printed language.
 - security_stamp: indicates the type of security stamp of the card.
 - loyalty: loyalty of the card. A card can have no loyalty or a special character loyalty.
 - watermark: indicates the type of water mark of the card.
 - produced_mana: indicates if the card produces mana and the type of mana.
 - color_indicator: is a color printed below the illustration. Usually found in nonland cards without mana cost.
 - content_warning: is 1 if there is a content warning.
 - life_modifier: life modifier of the card.
 - hand_modifier: hand modifier of the card.
