"""Create country to region mapping table."""

import sqlite3
from pathlib import Path

DB_PATH = (
    Path(__file__).parent.parent
    / "wikidata_sparql_scripts"
    / "instance_properties"
    / "output"
    / "instance_properties.db"
)

# Country to region mapping
COUNTRY_REGION_MAPPING = {
    # Eastern Europe - Balkans
    "Bulgaria": ("Eastern Europe", "Balkans"),
    "Greece": ("Eastern Europe", "Balkans"),
    "Albania": ("Eastern Europe", "Balkans"),
    "Montenegro": ("Eastern Europe", "Balkans"),
    "Serbia": ("Eastern Europe", "Balkans"),
    "Bosnia and Herzegovina": ("Eastern Europe", "Balkans"),
    "Croatia": ("Eastern Europe", "Balkans"),
    "North Macedonia": ("Eastern Europe", "Balkans"),
    "Kosovo": ("Eastern Europe", "Balkans"),
    "Slovenia": ("Eastern Europe", "Balkans"),
    # Eastern Europe - Central Europe
    "Latvia": ("Eastern Europe", "Central Europe"),
    "Estonia": ("Eastern Europe", "Central Europe"),
    "Slovakia": ("Eastern Europe", "Central Europe"),
    "Lithuania": ("Eastern Europe", "Central Europe"),
    "Czech Republic": ("Eastern Europe", "Central Europe"),
    "Czechia": ("Eastern Europe", "Central Europe"),
    "Poland": ("Eastern Europe", "Central Europe"),
    "Hungary": ("Eastern Europe", "Central Europe"),
    # Eastern Europe - East Slavic
    "Belarus": ("Eastern Europe", "East Slavic"),
    "Russia": ("Eastern Europe", "East Slavic"),
    "Russian Federation": ("Eastern Europe", "East Slavic"),
    "Ukraine": ("Eastern Europe", "East Slavic"),
    # Western Europe - British Islands
    "Ireland": ("Western Europe", "British Islands"),
    "United Kingdom": ("Western Europe", "British Islands"),
    # Western Europe - France
    "France": ("Western Europe", "France"),
    # Western Europe - German World
    "Germany": ("Western Europe", "German World"),
    "Switzerland": ("Western Europe", "German World"),
    "Austria": ("Western Europe", "German World"),
    # Western Europe - Portugal
    "Portugal": ("Western Europe", "Portugal"),
    # Western Europe - Spain
    "Spain": ("Western Europe", "Spain"),
    # Western Europe - Italy
    "Italy": ("Western Europe", "Italy"),
    # Western Europe - Low Countries
    "Netherlands": ("Western Europe", "Low Countries"),
    "Belgium": ("Western Europe", "Low Countries"),
    "Luxembourg": ("Western Europe", "Low Countries"),
    # Western Europe - Nordic
    "Denmark": ("Western Europe", "Nordic Countries"),
    "Norway": ("Western Europe", "Nordic Countries"),
    "Sweden": ("Western Europe", "Nordic Countries"),
    "Finland": ("Western Europe", "Nordic Countries"),
    "Iceland": ("Western Europe", "Nordic Countries"),
    # Middle-East and Africa (MENA) - Arabic World
    "Tunisia": ("MENA", "Arabic World"),
    "Algeria": ("MENA", "Arabic World"),
    "Morocco": ("MENA", "Arabic World"),
    "Libya": ("MENA", "Arabic World"),
    "Egypt": ("MENA", "Arabic World"),
    "Palestine": ("MENA", "Arabic World"),
    "Israel": ("MENA", "Arabic World"),
    "Lebanon": ("MENA", "Arabic World"),
    "Syria": ("MENA", "Arabic World"),
    "Syrian Arab Republic": ("MENA", "Arabic World"),
    "Jordan": ("MENA", "Arabic World"),
    "Iraq": ("MENA", "Arabic World"),
    "Kuwait": ("MENA", "Arabic World"),
    "Oman": ("MENA", "Arabic World"),
    "United Arab Emirates": ("MENA", "Arabic World"),
    "Saudi Arabia": ("MENA", "Arabic World"),
    "Bahrain": ("MENA", "Arabic World"),
    "Yemen": ("MENA", "Arabic World"),
    "Qatar": ("MENA", "Arabic World"),
    # MENA - Persian World
    "Iran": ("MENA", "Persian World"),
    "Afghanistan": ("MENA", "Persian World"),
    "Kyrgyzstan": ("MENA", "Persian World"),
    "Uzbekistan": ("MENA", "Persian World"),
    "Turkmenistan": ("MENA", "Persian World"),
    "Azerbaijan": ("MENA", "Persian World"),
    "Tajikistan": ("MENA", "Persian World"),
    "Kazakhstan": ("MENA", "Persian World"),
    # Asia - Chinese World
    "China": ("Asia", "Chinese World"),
    "Mongolia": ("Asia", "Chinese World"),
    "Taiwan": ("Asia", "Chinese World"),
    "Hong Kong": ("Asia", "Chinese World"),
    "Macau": ("Asia", "Chinese World"),
    # Asia - Indian World
    "India": ("Asia", "Indian World"),
    "Pakistan": ("Asia", "Indian World"),
    "Bangladesh": ("Asia", "Indian World"),
    "Sri Lanka": ("Asia", "Indian World"),
    "Nepal": ("Asia", "Indian World"),
    "Bhutan": ("Asia", "Indian World"),
    "Maldives": ("Asia", "Indian World"),
    # Asia - Japan
    "Japan": ("Asia", "Japan"),
    # Asia - Korea
    "Korea": ("Asia", "Korea"),
    "South Korea": ("Asia", "Korea"),
    "North Korea": ("Asia", "Korea"),
    # Asia - Southeast Asia
    "Vietnam": ("Asia", "Southeast Asia"),
    "Thailand": ("Asia", "Southeast Asia"),
    "Myanmar": ("Asia", "Southeast Asia"),
    "Cambodia": ("Asia", "Southeast Asia"),
    "Laos": ("Asia", "Southeast Asia"),
    "Malaysia": ("Asia", "Southeast Asia"),
    "Singapore": ("Asia", "Southeast Asia"),
    "Indonesia": ("Asia", "Southeast Asia"),
    "Philippines": ("Asia", "Southeast Asia"),
    "Brunei": ("Asia", "Southeast Asia"),
    "East Timor": ("Asia", "Southeast Asia"),
    "Timor-Leste": ("Asia", "Southeast Asia"),
    # North America
    "United States": ("Americas", "North America"),
    "United States of America": ("Americas", "North America"),
    "Canada": ("Americas", "North America"),
    # Central America & Caribbean
    "Mexico": ("Americas", "Central America & Caribbean"),
    "Guatemala": ("Americas", "Central America & Caribbean"),
    "Belize": ("Americas", "Central America & Caribbean"),
    "Honduras": ("Americas", "Central America & Caribbean"),
    "El Salvador": ("Americas", "Central America & Caribbean"),
    "Nicaragua": ("Americas", "Central America & Caribbean"),
    "Costa Rica": ("Americas", "Central America & Caribbean"),
    "Panama": ("Americas", "Central America & Caribbean"),
    "Cuba": ("Americas", "Central America & Caribbean"),
    "Jamaica": ("Americas", "Central America & Caribbean"),
    "Haiti": ("Americas", "Central America & Caribbean"),
    "Dominican Republic": ("Americas", "Central America & Caribbean"),
    "Puerto Rico": ("Americas", "Central America & Caribbean"),
    "Trinidad and Tobago": ("Americas", "Central America & Caribbean"),
    "Bahamas": ("Americas", "Central America & Caribbean"),
    "Barbados": ("Americas", "Central America & Caribbean"),
    # South America
    "Brazil": ("Americas", "South America"),
    "Argentina": ("Americas", "South America"),
    "Chile": ("Americas", "South America"),
    "Peru": ("Americas", "South America"),
    "Colombia": ("Americas", "South America"),
    "Venezuela": ("Americas", "South America"),
    "Ecuador": ("Americas", "South America"),
    "Bolivia": ("Americas", "South America"),
    "Paraguay": ("Americas", "South America"),
    "Uruguay": ("Americas", "South America"),
    "Guyana": ("Americas", "South America"),
    "Suriname": ("Americas", "South America"),
    # Sub-Saharan Africa - West Africa
    "Nigeria": ("Sub-Saharan Africa", "West Africa"),
    "Ghana": ("Sub-Saharan Africa", "West Africa"),
    "Senegal": ("Sub-Saharan Africa", "West Africa"),
    "Mali": ("Sub-Saharan Africa", "West Africa"),
    "Burkina Faso": ("Sub-Saharan Africa", "West Africa"),
    "Niger": ("Sub-Saharan Africa", "West Africa"),
    "Ivory Coast": ("Sub-Saharan Africa", "West Africa"),
    "Côte d'Ivoire": ("Sub-Saharan Africa", "West Africa"),
    "Guinea": ("Sub-Saharan Africa", "West Africa"),
    "Benin": ("Sub-Saharan Africa", "West Africa"),
    "Togo": ("Sub-Saharan Africa", "West Africa"),
    "Sierra Leone": ("Sub-Saharan Africa", "West Africa"),
    "Liberia": ("Sub-Saharan Africa", "West Africa"),
    "Mauritania": ("Sub-Saharan Africa", "West Africa"),
    "Gambia": ("Sub-Saharan Africa", "West Africa"),
    "Guinea-Bissau": ("Sub-Saharan Africa", "West Africa"),
    "Cape Verde": ("Sub-Saharan Africa", "West Africa"),
    # Sub-Saharan Africa - East Africa
    "Kenya": ("Sub-Saharan Africa", "East Africa"),
    "Tanzania": ("Sub-Saharan Africa", "East Africa"),
    "Uganda": ("Sub-Saharan Africa", "East Africa"),
    "Ethiopia": ("Sub-Saharan Africa", "East Africa"),
    "Eritrea": ("Sub-Saharan Africa", "East Africa"),
    "Somalia": ("Sub-Saharan Africa", "East Africa"),
    "Djibouti": ("Sub-Saharan Africa", "East Africa"),
    "Rwanda": ("Sub-Saharan Africa", "East Africa"),
    "Burundi": ("Sub-Saharan Africa", "East Africa"),
    "South Sudan": ("Sub-Saharan Africa", "East Africa"),
    "Sudan": ("Sub-Saharan Africa", "East Africa"),
    "Madagascar": ("Sub-Saharan Africa", "East Africa"),
    "Mauritius": ("Sub-Saharan Africa", "East Africa"),
    "Seychelles": ("Sub-Saharan Africa", "East Africa"),
    "Comoros": ("Sub-Saharan Africa", "East Africa"),
    "Malawi": ("Sub-Saharan Africa", "East Africa"),
    "Mozambique": ("Sub-Saharan Africa", "East Africa"),
    # Sub-Saharan Africa - Central Africa
    "Democratic Republic of the Congo": ("Sub-Saharan Africa", "Central Africa"),
    "Republic of the Congo": ("Sub-Saharan Africa", "Central Africa"),
    "Congo": ("Sub-Saharan Africa", "Central Africa"),
    "Cameroon": ("Sub-Saharan Africa", "Central Africa"),
    "Central African Republic": ("Sub-Saharan Africa", "Central Africa"),
    "Chad": ("Sub-Saharan Africa", "Central Africa"),
    "Gabon": ("Sub-Saharan Africa", "Central Africa"),
    "Equatorial Guinea": ("Sub-Saharan Africa", "Central Africa"),
    "São Tomé and Príncipe": ("Sub-Saharan Africa", "Central Africa"),
    "Angola": ("Sub-Saharan Africa", "Central Africa"),
    # Sub-Saharan Africa - Southern Africa
    "South Africa": ("Sub-Saharan Africa", "Southern Africa"),
    "Namibia": ("Sub-Saharan Africa", "Southern Africa"),
    "Botswana": ("Sub-Saharan Africa", "Southern Africa"),
    "Zimbabwe": ("Sub-Saharan Africa", "Southern Africa"),
    "Zambia": ("Sub-Saharan Africa", "Southern Africa"),
    "Lesotho": ("Sub-Saharan Africa", "Southern Africa"),
    "Eswatini": ("Sub-Saharan Africa", "Southern Africa"),
    "Swaziland": ("Sub-Saharan Africa", "Southern Africa"),
    # Oceania
    "Australia": ("Oceania", "Australia & New Zealand"),
    "New Zealand": ("Oceania", "Australia & New Zealand"),
    "Papua New Guinea": ("Oceania", "Pacific Islands"),
    "Fiji": ("Oceania", "Pacific Islands"),
    "Solomon Islands": ("Oceania", "Pacific Islands"),
    "Vanuatu": ("Oceania", "Pacific Islands"),
    "Samoa": ("Oceania", "Pacific Islands"),
    "Tonga": ("Oceania", "Pacific Islands"),
    # Caucasus
    "Georgia": ("Eastern Europe", "Caucasus"),
    "Armenia": ("Eastern Europe", "Caucasus"),
    "Turkey": ("MENA", "Turkey"),
    "Cyprus": ("MENA", "Turkey"),
}


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Drop and create table
    cursor.execute("DROP TABLE IF EXISTS country_region_mapping")
    cursor.execute(
        """
        CREATE TABLE country_region_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country TEXT UNIQUE,
            macro_region TEXT,
            region TEXT
        )
    """
    )

    # Insert mappings
    for country, (macro_region, region) in COUNTRY_REGION_MAPPING.items():
        cursor.execute(
            "INSERT INTO country_region_mapping (country, macro_region, region) VALUES (?, ?, ?)",
            (country, macro_region, region),
        )

    conn.commit()

    # Stats
    cursor.execute("SELECT COUNT(*) FROM country_region_mapping")
    total = cursor.fetchone()[0]

    cursor.execute(
        "SELECT macro_region, COUNT(*) FROM country_region_mapping GROUP BY macro_region"
    )
    print(f"Created country_region_mapping with {total} countries:\n")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} countries")

    conn.close()
    print(f"\nDone!")


if __name__ == "__main__":
    main()
